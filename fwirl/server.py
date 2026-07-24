import asyncio
import base64
import glob
import html
import os
import pickle
import sys
import tempfile
import threading
from queue import Queue

import psutil
from aiohttp import web
from coolname import generate_slug
from loguru import logger

from .message import __RABBIT_URL__, get_msg, publish_msg

__SERVERPORT__ = 8081


def request_graph(graph_key, msg, rabbit_url=__RABBIT_URL__, wait_for_response=True):
    payload = dict(msg)
    queue = None
    if wait_for_response:
        resp_name = f"graph-{generate_slug(2)}"
        payload["resp_queue"] = resp_name
        queue = Queue()
    publish_msg(graph_key, payload, rabbit_url)
    if not wait_for_response:
        return None
    get_msg(payload["resp_queue"], queue, rabbit_url)
    return queue.get()["response"]


def getgraph(graph_key, rabbit_url=__RABBIT_URL__):
    return request_graph(graph_key, {"type": "graph"}, rabbit_url=rabbit_url)


def dashboard_html(graph_key):
    safe_graph_key = html.escape(graph_key)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>fwirl dashboard - {safe_graph_key}</title>
  <link href="https://unpkg.com/tabulator-tables@6.3.0/dist/css/tabulator_midnight.min.css" rel="stylesheet">
  <style>
    :root {{
      --bg: #0f172a;
      --panel: #1e293b;
      --panel-2: #111827;
      --text: #e2e8f0;
      --muted: #94a3b8;
      --accent: #38bdf8;
    }}
    body {{
      margin: 0;
      font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, sans-serif;
      background: var(--bg);
      color: var(--text);
    }}
    .page {{
      display: grid;
      grid-template-rows: auto 1fr;
      height: 100vh;
      gap: 12px;
      padding: 12px;
      box-sizing: border-box;
    }}
    .header {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 12px 16px;
      background: var(--panel);
      border-radius: 12px;
    }}
    .header h1 {{ margin: 0; font-size: 18px; }}
    .header .meta {{ color: var(--muted); font-size: 13px; }}
    .layout {{
      display: grid;
      grid-template-columns: minmax(500px, 2fr) minmax(360px, 1fr);
      gap: 12px;
      min-height: 0;
    }}
    .panel {{
      background: var(--panel);
      border-radius: 12px;
      padding: 12px;
      min-height: 0;
      display: flex;
      flex-direction: column;
      gap: 8px;
    }}
    #graph {{ flex: 1; min-height: 360px; border-radius: 10px; background: var(--panel-2); }}
    #assets-table {{ flex: 1; min-height: 280px; }}
    .section-title {{ font-size: 14px; font-weight: 600; color: var(--muted); }}
    .detail-grid {{ display: grid; grid-template-columns: 120px 1fr; gap: 6px 10px; font-size: 13px; }}
    .detail-grid .k {{ color: var(--muted); }}
    .controls {{ display: grid; gap: 6px; }}
    .controls input, .controls button {{
      border-radius: 8px; border: 1px solid #334155; padding: 8px; background: #0b1220; color: var(--text);
    }}
    .controls button {{ cursor: pointer; background: #0b2f40; border-color: #155e75; }}
    .controls button.secondary {{ background: #1f2937; border-color: #374151; }}
    .actions {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 6px; }}
    .status-pill {{
      display: inline-block; padding: 2px 8px; border-radius: 999px; font-size: 12px; font-weight: 600;
    }}
  </style>
  <script src="https://unpkg.com/vis-network@9.1.10/dist/vis-network.min.js"></script>
  <script src="https://unpkg.com/tabulator-tables@6.3.0/dist/js/tabulator.min.js"></script>
</head>
<body>
  <div class="page">
    <div class="header">
      <h1>fwirl Dashboard</h1>
      <div class="meta">Graph: <b id="graph-key">{safe_graph_key}</b> · <span id="summary">Loading...</span></div>
    </div>
    <div class="layout">
      <div class="panel">
        <div class="section-title">Asset Graph</div>
        <div id="graph"></div>
      </div>
      <div class="panel">
        <div class="section-title">Asset Details</div>
        <div id="asset-detail" class="detail-grid"><div class="k">Selection</div><div>None</div></div>
        <div class="actions">
          <button id="build-btn">Build</button>
          <button id="refresh-btn">Refresh</button>
          <button id="remove-btn" class="secondary">Remove</button>
        </div>
        <div class="section-title">Edit Dependencies</div>
        <div class="controls">
          <input id="deps-input" placeholder="comma-separated parent asset keys" />
          <button id="deps-save-btn">Save dependencies</button>
        </div>
        <div class="section-title">Add Asset</div>
        <div class="controls">
          <input id="new-key" placeholder="asset key" />
          <input id="new-deps" placeholder="dependencies (comma-separated keys)" />
          <input id="new-group" placeholder="group (optional)" />
          <input id="new-subgroup" placeholder="subgroup (optional)" />
          <button id="add-btn">Add asset</button>
        </div>
      </div>
    </div>
    <div class="panel">
      <div class="section-title">Assets</div>
      <div id="assets-table"></div>
    </div>
  </div>
  <script>
    const GRAPH_KEY = document.getElementById("graph-key").textContent;
    const statusColors = {{
      Current: "#22c55e",
      Stale: "#facc15",
      Building: "#3b82f6",
      Paused: "#64748b",
      UpstreamStopped: "#475569",
      Unavailable: "#9ca3af",
      Failed: "#ef4444"
    }};

    let selectedAsset = null;
    let latestSnapshot = null;
    const nodeData = new vis.DataSet([]);
    const edgeData = new vis.DataSet([]);
    const network = new vis.Network(document.getElementById("graph"), {{ nodes: nodeData, edges: edgeData }}, {{
      nodes: {{ shape: "dot", size: 18, font: {{ color: "#e2e8f0" }} }},
      edges: {{ arrows: "to", color: "#64748b", smooth: true }},
      interaction: {{ hover: true, navigationButtons: true }},
      physics: {{ stabilization: false }}
    }});

    const table = new Tabulator("#assets-table", {{
      layout: "fitColumns",
      data: [],
      selectableRows: 1,
      columns: [
        {{ title: "Key", field: "key", sorter: "string" }},
        {{ title: "Status", field: "status", sorter: "string" }},
        {{ title: "Type", field: "type", sorter: "string" }},
        {{ title: "Group", field: "group", sorter: "string" }},
        {{ title: "Subgroup", field: "subgroup", sorter: "string" }},
        {{ title: "Last Build", field: "last_build_timestamp", sorter: "string" }},
        {{ title: "Parents", field: "parents", sorter: "string" }}
      ]
    }});

    table.on("rowClick", (_, row) => {{
      const asset = row.getData();
      if (!asset || !asset.key) return;
      selectedAsset = asset.key;
      fetchAssetDetail(selectedAsset);
      network.selectNodes([selectedAsset]);
      network.focus(selectedAsset, {{ animation: true, scale: 1.0 }});
    }});

    network.on("click", (params) => {{
      if (params.nodes.length === 0) return;
      const id = params.nodes[0];
      if (network.isCluster(id)) return;
      selectedAsset = id;
      fetchAssetDetail(id);
      const row = table.getRow(id);
      if (row) row.select();
    }});

    network.on("doubleClick", (params) => {{
      if (params.nodes.length === 0) return;
      const id = params.nodes[0];
      if (network.isCluster(id)) network.openCluster(id);
    }});

    function csvToList(v) {{
      return (v || "").split(",").map(s => s.trim()).filter(Boolean);
    }}

    function formatStatus(status) {{
      const color = statusColors[status] || "#9ca3af";
      return `<span class="status-pill" style="background:${{color}}22;color:${{color}};">${{status}}</span>`;
    }}

    function renderDetails(asset) {{
      if (!asset) {{
        document.getElementById("asset-detail").innerHTML = "<div class='k'>Selection</div><div>None</div>";
        document.getElementById("deps-input").value = "";
        return;
      }}
      document.getElementById("asset-detail").innerHTML = `
        <div class="k">Key</div><div>${{asset.key}}</div>
        <div class="k">Status</div><div>${{formatStatus(asset.status)}}</div>
        <div class="k">Type</div><div>${{asset.type || ""}}</div>
        <div class="k">Group</div><div>${{asset.group ?? ""}}</div>
        <div class="k">Subgroup</div><div>${{asset.subgroup ?? ""}}</div>
        <div class="k">Last Build</div><div>${{asset.last_build_timestamp ?? "N/A"}}</div>
        <div class="k">Timestamp</div><div>${{asset.timestamp ?? "N/A"}}</div>
        <div class="k">Message</div><div>${{asset.message || ""}}</div>
        <div class="k">Parents</div><div>${{(asset.parents || []).join(", ")}}</div>
        <div class="k">Children</div><div>${{(asset.children || []).join(", ")}}</div>
      `;
      document.getElementById("deps-input").value = (asset.parents || []).join(", ");
    }}

    async function api(path, opts={{}}) {{
      const res = await fetch(path, {{
        headers: {{ "Content-Type": "application/json" }},
        ...opts
      }});
      if (!res.ok) {{
        const txt = await res.text();
        throw new Error(txt || `HTTP ${{res.status}}`);
      }}
      if (res.status === 204) return null;
      return await res.json();
    }}

    async function fetchSnapshot() {{
      latestSnapshot = await api(`/api/graphs/${{encodeURIComponent(GRAPH_KEY)}}/snapshot`);
      const nodes = latestSnapshot.nodes.map(n => ({{
        id: n.key,
        label: n.key,
        color: {{ background: statusColors[n.status] || "#9ca3af", border: "#0b1220" }},
        title: `${{n.key}} (${{n.status}})`
      }}));
      const edges = latestSnapshot.edges.map(e => ({{
        id: `${{e.from}}->${{e.to}}`,
        from: e.from,
        to: e.to
      }}));
      nodeData.clear();
      edgeData.clear();
      nodeData.add(nodes);
      edgeData.add(edges);

      for (const candidate of latestSnapshot.collapse_candidates || []) {{
        const clusterId = `cluster:${{candidate.id}}`;
        const nodeSet = new Set(candidate.node_keys || []);
        network.cluster({{
          joinCondition: function(nodeOptions) {{ return nodeSet.has(nodeOptions.id); }},
          clusterNodeProperties: {{
            id: clusterId,
            label: `${{candidate.group ?? "ungrouped"}}/${{candidate.subgroup ?? "default"}} (${{candidate.status}})`,
            color: statusColors[candidate.status] || "#9ca3af",
            shape: "box"
          }}
        }});
      }}

      const tableRows = latestSnapshot.nodes.map(n => ({{
        id: n.key,
        key: n.key,
        status: n.status,
        type: n.type || "",
        group: n.group ?? "",
        subgroup: n.subgroup ?? "",
        last_build_timestamp: n.last_build_timestamp ?? "",
        parents: (n.parents || []).join(", ")
      }}));
      table.setData(tableRows);
      document.getElementById("summary").textContent =
        `${{latestSnapshot.summary.asset_count}} assets · ${{latestSnapshot.summary.edge_count}} edges`;
    }}

    async function fetchAssetDetail(assetKey) {{
      const asset = await api(`/api/graphs/${{encodeURIComponent(GRAPH_KEY)}}/assets/${{encodeURIComponent(assetKey)}}`);
      renderDetails(asset);
    }}

    async function triggerAction(action) {{
      if (!selectedAsset) return;
      await api(`/api/graphs/${{encodeURIComponent(GRAPH_KEY)}}/assets/${{encodeURIComponent(selectedAsset)}}/${{action}}`, {{
        method: "POST"
      }});
      await fetchSnapshot();
      await fetchAssetDetail(selectedAsset);
    }}

    document.getElementById("build-btn").addEventListener("click", () => triggerAction("build"));
    document.getElementById("refresh-btn").addEventListener("click", () => triggerAction("refresh"));

    document.getElementById("remove-btn").addEventListener("click", async () => {{
      if (!selectedAsset) return;
      await api(`/api/graphs/${{encodeURIComponent(GRAPH_KEY)}}/assets/${{encodeURIComponent(selectedAsset)}}`, {{
        method: "DELETE"
      }});
      selectedAsset = null;
      renderDetails(null);
      await fetchSnapshot();
    }});

    document.getElementById("deps-save-btn").addEventListener("click", async () => {{
      if (!selectedAsset) return;
      await api(`/api/graphs/${{encodeURIComponent(GRAPH_KEY)}}/assets/${{encodeURIComponent(selectedAsset)}}/dependencies`, {{
        method: "PUT",
        body: JSON.stringify({{ dependencies: csvToList(document.getElementById("deps-input").value) }})
      }});
      await fetchSnapshot();
      await fetchAssetDetail(selectedAsset);
    }});

    document.getElementById("add-btn").addEventListener("click", async () => {{
      const key = document.getElementById("new-key").value.trim();
      if (!key) return;
      await api(`/api/graphs/${{encodeURIComponent(GRAPH_KEY)}}/assets`, {{
        method: "POST",
        body: JSON.stringify({{
          asset_key: key,
          dependencies: csvToList(document.getElementById("new-deps").value),
          group: document.getElementById("new-group").value.trim() || null,
          subgroup: document.getElementById("new-subgroup").value.trim() || null
        }})
      }});
      document.getElementById("new-key").value = "";
      document.getElementById("new-deps").value = "";
      document.getElementById("new-group").value = "";
      document.getElementById("new-subgroup").value = "";
      await fetchSnapshot();
    }});

    (async () => {{
      try {{
        await fetchSnapshot();
      }} catch (e) {{
        document.getElementById("summary").textContent = `Error: ${{e.message}}`;
      }}
      setInterval(fetchSnapshot, 10000);
    }})();
  </script>
</body>
</html>"""


def aiohttp_server():
    async def get_svg(request):
        graph_key = request.match_info["graph_key"]
        encoded = await asyncio.to_thread(getgraph, graph_key)
        decoded = base64.b64decode(encoded)
        svg = pickle.loads(decoded)
        return web.Response(text=svg.decode("utf-8"), content_type="text/html")

    async def get_ui(request):
        graph_key = request.match_info["graph_key"]
        return web.Response(text=dashboard_html(graph_key), content_type="text/html")

    async def get_snapshot(request):
        graph_key = request.match_info["graph_key"]
        response = await asyncio.to_thread(request_graph, graph_key, {"type": "graph_snapshot"})
        return web.json_response(response)

    async def get_asset_detail(request):
        graph_key = request.match_info["graph_key"]
        asset_key = request.match_info["asset_key"]
        response = await asyncio.to_thread(
            request_graph, graph_key, {"type": "asset_detail", "asset_key": asset_key}
        )
        if response is None:
            return web.json_response({"error": "Asset not found"}, status=404)
        return web.json_response(response)

    async def trigger_build(request):
        graph_key = request.match_info["graph_key"]
        asset_key = request.match_info["asset_key"]
        await asyncio.to_thread(request_graph, graph_key, {"type": "build", "asset_key": asset_key}, wait_for_response=False)
        return web.json_response({"ok": True})

    async def trigger_refresh(request):
        graph_key = request.match_info["graph_key"]
        asset_key = request.match_info["asset_key"]
        await asyncio.to_thread(request_graph, graph_key, {"type": "refresh", "asset_key": asset_key}, wait_for_response=False)
        return web.json_response({"ok": True})

    async def add_asset(request):
        graph_key = request.match_info["graph_key"]
        body = await request.json()
        response = await asyncio.to_thread(
            request_graph,
            graph_key,
            {
                "type": "add_asset",
                "asset_key": body["asset_key"],
                "dependencies": body.get("dependencies", []),
                "group": body.get("group"),
                "subgroup": body.get("subgroup"),
                "allow_retry": body.get("allow_retry", True),
            },
        )
        if response.get("ok"):
            return web.json_response(response)
        return web.json_response(response, status=400)

    async def remove_asset(request):
        graph_key = request.match_info["graph_key"]
        asset_key = request.match_info["asset_key"]
        response = await asyncio.to_thread(
            request_graph, graph_key, {"type": "remove_asset", "asset_key": asset_key}
        )
        if response.get("ok"):
            return web.json_response(response)
        return web.json_response(response, status=400)

    async def update_dependencies(request):
        graph_key = request.match_info["graph_key"]
        asset_key = request.match_info["asset_key"]
        body = await request.json()
        response = await asyncio.to_thread(
            request_graph,
            graph_key,
            {"type": "update_asset_dependencies", "asset_key": asset_key, "dependencies": body.get("dependencies", [])},
        )
        if response.get("ok"):
            return web.json_response(response)
        return web.json_response(response, status=400)

    app = web.Application()
    app.add_routes(
        [
            web.get("/graphs/{graph_key}", get_svg),
            web.get("/ui/{graph_key}", get_ui),
            web.get("/api/graphs/{graph_key}/snapshot", get_snapshot),
            web.get("/api/graphs/{graph_key}/assets/{asset_key}", get_asset_detail),
            web.post("/api/graphs/{graph_key}/assets/{asset_key}/build", trigger_build),
            web.post("/api/graphs/{graph_key}/assets/{asset_key}/refresh", trigger_refresh),
            web.post("/api/graphs/{graph_key}/assets", add_asset),
            web.delete("/api/graphs/{graph_key}/assets/{asset_key}", remove_asset),
            web.put("/api/graphs/{graph_key}/assets/{asset_key}/dependencies", update_dependencies),
        ]
    )
    runner = web.AppRunner(app)
    return runner


def run_server(runner):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(runner.setup())
    site = web.TCPSite(runner, "127.0.0.1", __SERVERPORT__)
    loop.run_until_complete(site.start())
    loop.run_forever()


def start_webserver():
    fpid = os.fork()
    if fpid != 0:
        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, prefix="fwirlserverpid_", dir=tempfile.gettempdir()
        ) as temp_file:
            temp_file.write(str(fpid))
        sys.exit(0)

    logger.info(f"Starting webserver on port {__SERVERPORT__}...")
    t = threading.Thread(target=run_server, args=(aiohttp_server(),))
    t.start()


def stop_webserver():
    temp_dir = tempfile.gettempdir()
    pid_files = glob.glob(os.path.join(temp_dir, "fwirlserverpid_*"))

    logger.info("Stopping webserver...")

    for pid_file in pid_files:
        try:
            with open(pid_file, "r") as temp_file:
                pid = int(temp_file.read().strip())
        except Exception:
            pid = None

        if pid is None:
            continue

        try:
            process = psutil.Process(pid)
            process.terminate()
            process.wait(timeout=10)
            os.unlink(pid_file)
        except psutil.TimeoutExpired:
            logger.info(f"Failed to stop webserver process {pid}, timeout expired.")
        except psutil.AccessDenied:
            logger.info(f"Failed to stop webserver process {pid}, access denied.")
        except Exception:
            pass
