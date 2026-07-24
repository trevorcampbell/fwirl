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
      --danger: #ef4444;
    }}
    body {{
      margin: 0;
      font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, sans-serif;
      background: var(--bg);
      color: var(--text);
    }}
    .page {{
      display: grid;
      grid-template-rows: auto 1fr auto;
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
      gap: 12px;
    }}
    .header h1 {{ margin: 0; font-size: 18px; }}
    .header .meta {{ color: var(--muted); font-size: 13px; }}
    .layout {{
      display: grid;
      grid-template-columns: minmax(520px, 2fr) minmax(380px, 1fr);
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
    #graph-wrap {{
      position: relative;
      flex: 1;
      min-height: 420px;
      border-radius: 10px;
      background: var(--panel-2);
      overflow: hidden;
    }}
    #graph {{ position: absolute; inset: 0; }}
    #selection-box {{
      position: absolute;
      border: 1px dashed var(--accent);
      background: rgba(56, 189, 248, 0.12);
      pointer-events: none;
      display: none;
    }}
    #assets-table {{ flex: 1; min-height: 280px; }}
    .section-title {{ font-size: 14px; font-weight: 600; color: var(--muted); }}
    .detail-grid {{ display: grid; grid-template-columns: 120px 1fr; gap: 6px 10px; font-size: 13px; }}
    .detail-grid .k {{ color: var(--muted); }}
    .controls {{ display: grid; gap: 6px; }}
    .controls input, .controls textarea, .controls button {{
      border-radius: 8px;
      border: 1px solid #334155;
      padding: 8px;
      background: #0b1220;
      color: var(--text);
      font: inherit;
    }}
    .controls textarea {{ min-height: 80px; resize: vertical; }}
    .controls button {{ cursor: pointer; background: #0b2f40; border-color: #155e75; }}
    .controls button.secondary {{ background: #1f2937; border-color: #374151; }}
    .controls button.danger {{ background: #3f1117; border-color: #7f1d1d; }}
    .actions {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 6px; }}
    .status-pill {{
      display: inline-block; padding: 2px 8px; border-radius: 999px; font-size: 12px; font-weight: 600;
    }}
    .toolbar {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 8px;
      color: var(--muted);
      font-size: 13px;
    }}
    details {{
      background: #0b1220;
      border: 1px solid #334155;
      border-radius: 8px;
      padding: 8px;
    }}
    pre {{
      margin: 0;
      white-space: pre-wrap;
      word-break: break-word;
      font-size: 12px;
      color: #cbd5e1;
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
        <div class="toolbar">
          <div>Graph view</div>
          <div id="selection-summary">0 selected</div>
        </div>
        <div id="graph-wrap">
          <div id="graph"></div>
          <div id="selection-box"></div>
        </div>
      </div>
      <div class="panel">
        <div class="section-title">Asset Details</div>
        <div id="asset-detail" class="detail-grid"><div class="k">Selection</div><div>None</div></div>
        <div class="actions">
          <button id="build-btn">Build</button>
          <button id="refresh-btn">Refresh</button>
          <button id="copy-btn">Copy</button>
          <button id="remove-btn" class="danger">Remove</button>
        </div>
        <div class="section-title">Edit Dependencies</div>
        <div class="controls">
          <input id="deps-input" placeholder="comma-separated parent asset keys" />
          <button id="deps-save-btn">Save dependencies</button>
        </div>
        <div class="section-title">Properties</div>
        <details open>
          <summary>Edit selected asset properties (JSON)</summary>
          <div class="controls" style="margin-top:8px;">
            <textarea id="properties-editor" placeholder='{{"owner":"team-a"}}'></textarea>
            <button id="properties-save-btn">Save properties</button>
          </div>
        </details>
        <div class="section-title">Add Asset</div>
        <div class="controls">
          <input id="new-key" placeholder="asset key" />
          <input id="new-deps" placeholder="dependencies (comma-separated keys)" />
          <input id="new-group" placeholder="group (optional)" />
          <input id="new-subgroup" placeholder="subgroup (optional)" />
          <textarea id="new-properties" placeholder='properties JSON, e.g. {{"owner":"team-a"}}'></textarea>
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
    const graphWrap = document.getElementById("graph-wrap");
    const graphEl = document.getElementById("graph");
    const selectionBox = document.getElementById("selection-box");
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
    let selectedAssets = [];
    let latestSnapshot = null;
    let boxSelection = null;
    let connectDrag = null;

    const nodeData = new vis.DataSet([]);
    const edgeData = new vis.DataSet([]);
    const network = new vis.Network(graphEl, {{ nodes: nodeData, edges: edgeData }}, {{
      nodes: {{
        shape: "dot",
        size: 18,
        borderWidth: 1,
        font: {{ color: "#e2e8f0" }}
      }},
      edges: {{
        arrows: "to",
        color: "#64748b",
        smooth: true
      }},
      interaction: {{
        hover: true,
        navigationButtons: true,
        multiselect: true,
        dragNodes: false,
        dragView: false
      }},
      physics: {{ stabilization: false }}
    }});

    const table = new Tabulator("#assets-table", {{
      layout: "fitColumns",
      data: [],
      selectableRows: true,
      columns: [
        {{ title: "Key", field: "key", sorter: "string" }},
        {{ title: "Status", field: "status", sorter: "string" }},
        {{ title: "Type", field: "type", sorter: "string" }},
        {{ title: "Group", field: "group", sorter: "string" }},
        {{ title: "Subgroup", field: "subgroup", sorter: "string" }},
        {{ title: "Last Build", field: "last_build_timestamp", sorter: "string" }},
        {{ title: "Parents", field: "parents", sorter: "string" }},
        {{ title: "Properties", field: "properties_text", sorter: "string", editor: "input" }}
      ],
      cellEdited: async function(cell) {{
        if (cell.getField() !== "properties_text") return;
        const row = cell.getRow().getData();
        try {{
          const properties = parseJsonField(cell.getValue(), {{}});
          await saveProperties(row.key, properties);
          await fetchSnapshot();
          if (selectedAssets.includes(row.key)) await fetchAssetDetail(row.key);
        }} catch (error) {{
          alert(error.message);
          await fetchSnapshot();
        }}
      }}
    }});

    table.on("rowClick", (_, row) => {{
      const asset = row.getData();
      if (!asset || !asset.key) return;
      setSelectedAssets([asset.key], true);
    }});

    network.on("click", (params) => {{
      if (boxSelection || connectDrag) return;
      if (params.nodes.length === 0) {{
        setSelectedAssets([]);
        return;
      }}
      const id = params.nodes[0];
      if (network.isCluster(id)) return;
      setSelectedAssets([id], true);
    }});

    network.on("doubleClick", (params) => {{
      if (params.nodes.length === 0) return;
      const id = params.nodes[0];
      if (network.isCluster(id)) network.openCluster(id);
    }});

    network.on("afterDrawing", (ctx) => {{
      if (!connectDrag) return;
      const originPosition = network.getPositions([connectDrag.origin])[connectDrag.origin];
      if (!originPosition) return;
      const start = originPosition;
      const end = connectDrag.currentCanvas;
      ctx.save();
      ctx.strokeStyle = statusColors.Stale;
      ctx.fillStyle = statusColors.Stale;
      ctx.lineWidth = 2;
      ctx.beginPath();
      ctx.moveTo(start.x, start.y);
      ctx.lineTo(end.x, end.y);
      ctx.stroke();
      const angle = Math.atan2(end.y - start.y, end.x - start.x);
      const headLength = 10;
      ctx.beginPath();
      ctx.moveTo(end.x, end.y);
      ctx.lineTo(end.x - headLength * Math.cos(angle - Math.PI / 6), end.y - headLength * Math.sin(angle - Math.PI / 6));
      ctx.lineTo(end.x - headLength * Math.cos(angle + Math.PI / 6), end.y - headLength * Math.sin(angle + Math.PI / 6));
      ctx.closePath();
      ctx.fill();
      ctx.restore();
    }});

    graphWrap.addEventListener("mousedown", (event) => {{
      const rect = graphWrap.getBoundingClientRect();
      const point = {{ x: event.clientX - rect.left, y: event.clientY - rect.top }};
      const nodeId = network.getNodeAt(point);
      if (nodeId && !network.isCluster(nodeId)) {{
        connectDrag = {{
          origin: nodeId,
          currentDom: point,
          currentCanvas: network.DOMtoCanvas(point)
        }};
        network.redraw();
        return;
      }}
      boxSelection = {{ start: point, current: point }};
      drawSelectionBox();
    }});

    window.addEventListener("mousemove", (event) => {{
      const rect = graphWrap.getBoundingClientRect();
      const point = {{ x: event.clientX - rect.left, y: event.clientY - rect.top }};
      if (connectDrag) {{
        connectDrag.currentDom = point;
        connectDrag.currentCanvas = network.DOMtoCanvas(point);
        network.redraw();
        return;
      }}
      if (!boxSelection) return;
      boxSelection.current = point;
      drawSelectionBox();
    }});

    window.addEventListener("mouseup", async (event) => {{
      const rect = graphWrap.getBoundingClientRect();
      const point = {{ x: event.clientX - rect.left, y: event.clientY - rect.top }};
      if (connectDrag) {{
        const origin = connectDrag.origin;
        const target = network.getNodeAt(point);
        connectDrag = null;
        network.redraw();
        if (target && target !== origin && !network.isCluster(target)) {{
          try {{
            await api(`/api/graphs/${{encodeURIComponent(GRAPH_KEY)}}/dependencies`, {{
              method: "POST",
              body: JSON.stringify({{ parent_key: origin, child_key: target }})
            }});
            await fetchSnapshot();
            await fetchAssetDetail(target);
            setSelectedAssets([target], false);
          }} catch (error) {{
            alert(error.message);
          }}
        }}
        return;
      }}
      if (!boxSelection) return;
      boxSelection.current = point;
      const selected = nodesWithinSelection(boxSelection);
      boxSelection = null;
      selectionBox.style.display = "none";
      setSelectedAssets(selected, selected.length === 1);
    }});

    function drawSelectionBox() {{
      if (!boxSelection) {{
        selectionBox.style.display = "none";
        return;
      }}
      const left = Math.min(boxSelection.start.x, boxSelection.current.x);
      const top = Math.min(boxSelection.start.y, boxSelection.current.y);
      const width = Math.abs(boxSelection.current.x - boxSelection.start.x);
      const height = Math.abs(boxSelection.current.y - boxSelection.start.y);
      selectionBox.style.left = `${{left}}px`;
      selectionBox.style.top = `${{top}}px`;
      selectionBox.style.width = `${{width}}px`;
      selectionBox.style.height = `${{height}}px`;
      selectionBox.style.display = width < 2 && height < 2 ? "none" : "block";
    }}

    function nodesWithinSelection(selection) {{
      const left = Math.min(selection.start.x, selection.current.x);
      const right = Math.max(selection.start.x, selection.current.x);
      const top = Math.min(selection.start.y, selection.current.y);
      const bottom = Math.max(selection.start.y, selection.current.y);
      return nodeData.getIds().filter((id) => {{
        if (network.isCluster(id)) return false;
        const pos = network.getPositions([id])[id];
        if (!pos) return false;
        const dom = network.canvasToDOM(pos);
        return dom.x >= left && dom.x <= right && dom.y >= top && dom.y <= bottom;
      }});
    }}

    function csvToList(v) {{
      return (v || "").split(",").map(s => s.trim()).filter(Boolean);
    }}

    function parseJsonField(value, fallback) {{
      const trimmed = (value || "").trim();
      if (!trimmed) return fallback;
      const parsed = JSON.parse(trimmed);
      if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {{
        throw new Error("Properties must be a JSON object");
      }}
      return parsed;
    }}

    function formatStatus(status) {{
      const color = statusColors[status] || "#9ca3af";
      return `<span class="status-pill" style="background:${{color}}22;color:${{color}};">${{status}}</span>`;
    }}

    function propertiesText(properties) {{
      return JSON.stringify(properties || {{}});
    }}

    function updateSelectionStyles() {{
      const selectedSet = new Set(selectedAssets);
      const updates = (latestSnapshot?.nodes || []).map((node) => ({{
        id: node.key,
        color: {{
          background: statusColors[node.status] || "#9ca3af",
          border: selectedSet.has(node.key) ? "#f8fafc" : "#0b1220"
        }},
        borderWidth: selectedSet.has(node.key) ? 4 : 1,
        shadow: selectedSet.has(node.key)
      }}));
      nodeData.update(updates);
      network.selectNodes(selectedAssets.filter(id => nodeData.get(id)));
      document.getElementById("selection-summary").textContent = `${{selectedAssets.length}} selected`;
      table.deselectRow();
      if (selectedAssets.length > 0) table.selectRow(selectedAssets);
    }}

    function renderDetails(asset) {{
      if (!asset) {{
        document.getElementById("asset-detail").innerHTML = "<div class='k'>Selection</div><div>None</div>";
        document.getElementById("deps-input").value = "";
        document.getElementById("properties-editor").value = "";
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
        <div class="k">Properties</div><pre>${{propertiesText(asset.properties)}}</pre>
      `;
      document.getElementById("deps-input").value = (asset.parents || []).join(", ");
      document.getElementById("properties-editor").value = JSON.stringify(asset.properties || {{}}, null, 2);
    }}

    async function api(path, opts={{}}) {{
      const res = await fetch(path, {{
        headers: {{ "Content-Type": "application/json" }},
        ...opts
      }});
      const text = await res.text();
      const data = text ? JSON.parse(text) : null;
      if (!res.ok) {{
        throw new Error(data?.error || text || `HTTP ${{res.status}}`);
      }}
      return data;
    }}

    async function fetchSnapshot() {{
      latestSnapshot = await api(`/api/graphs/${{encodeURIComponent(GRAPH_KEY)}}/snapshot`);
      const nodes = latestSnapshot.nodes.map(n => ({{
        id: n.key,
        label: n.key,
        color: {{ background: statusColors[n.status] || "#9ca3af", border: "#0b1220" }},
        title: `${{n.key}} (${{n.status}})`,
        properties: n.properties || {{}}
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
            shape: "box",
            borderWidth: 1
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
        parents: (n.parents || []).join(", "),
        properties_text: propertiesText(n.properties)
      }}));
      table.setData(tableRows);
      document.getElementById("summary").textContent =
        `${{latestSnapshot.summary.asset_count}} assets · ${{latestSnapshot.summary.edge_count}} edges`;
      selectedAssets = selectedAssets.filter(key => latestSnapshot.nodes.some(node => node.key === key));
      selectedAsset = selectedAssets.length > 0 ? selectedAssets[0] : null;
      updateSelectionStyles();
    }}

    async function fetchAssetDetail(assetKey) {{
      const asset = await api(`/api/graphs/${{encodeURIComponent(GRAPH_KEY)}}/assets/${{encodeURIComponent(assetKey)}}`);
      renderDetails(asset);
    }}

    async function saveProperties(assetKey, properties) {{
      await api(`/api/graphs/${{encodeURIComponent(GRAPH_KEY)}}/assets/${{encodeURIComponent(assetKey)}}/properties`, {{
        method: "PUT",
        body: JSON.stringify({{ properties }})
      }});
    }}

    async function setSelectedAssets(assetKeys, fetchDetail=true) {{
      selectedAssets = [...new Set(assetKeys)];
      selectedAsset = selectedAssets.length > 0 ? selectedAssets[0] : null;
      updateSelectionStyles();
      if (fetchDetail && selectedAsset) {{
        await fetchAssetDetail(selectedAsset);
      }} else if (!selectedAsset) {{
        renderDetails(null);
      }}
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

    document.getElementById("copy-btn").addEventListener("click", async () => {{
      if (selectedAssets.length === 0) return;
      const response = await api(`/api/graphs/${{encodeURIComponent(GRAPH_KEY)}}/assets/copy`, {{
        method: "POST",
        body: JSON.stringify({{ asset_keys: selectedAssets }})
      }});
      await fetchSnapshot();
      await setSelectedAssets(response.asset_keys || [], true);
    }});

    document.getElementById("remove-btn").addEventListener("click", async () => {{
      if (!selectedAsset) return;
      await api(`/api/graphs/${{encodeURIComponent(GRAPH_KEY)}}/assets/${{encodeURIComponent(selectedAsset)}}`, {{
        method: "DELETE"
      }});
      selectedAsset = null;
      selectedAssets = [];
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

    document.getElementById("properties-save-btn").addEventListener("click", async () => {{
      if (!selectedAsset) return;
      const properties = parseJsonField(document.getElementById("properties-editor").value, {{}});
      await saveProperties(selectedAsset, properties);
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
          subgroup: document.getElementById("new-subgroup").value.trim() || null,
          properties: parseJsonField(document.getElementById("new-properties").value, {{}})
        }})
      }});
      document.getElementById("new-key").value = "";
      document.getElementById("new-deps").value = "";
      document.getElementById("new-group").value = "";
      document.getElementById("new-subgroup").value = "";
      document.getElementById("new-properties").value = "";
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
        await asyncio.to_thread(
            request_graph, graph_key, {"type": "build", "asset_key": asset_key}, wait_for_response=False
        )
        return web.json_response({"ok": True})

    async def trigger_refresh(request):
        graph_key = request.match_info["graph_key"]
        asset_key = request.match_info["asset_key"]
        await asyncio.to_thread(
            request_graph, graph_key, {"type": "refresh", "asset_key": asset_key}, wait_for_response=False
        )
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
                "properties": body.get("properties", {}),
                "paused": body.get("paused", False),
            },
        )
        if response.get("ok"):
            return web.json_response(response)
        return web.json_response({"error": response.get("error", "Unable to add asset")}, status=400)

    async def copy_assets(request):
        graph_key = request.match_info["graph_key"]
        body = await request.json()
        response = await asyncio.to_thread(
            request_graph,
            graph_key,
            {"type": "copy_assets", "asset_keys": body.get("asset_keys", [])},
        )
        if response.get("ok"):
            return web.json_response(response)
        return web.json_response({"error": response.get("error", "Unable to copy assets")}, status=400)

    async def remove_asset(request):
        graph_key = request.match_info["graph_key"]
        asset_key = request.match_info["asset_key"]
        response = await asyncio.to_thread(
            request_graph, graph_key, {"type": "remove_asset", "asset_key": asset_key}
        )
        if response.get("ok"):
            return web.json_response(response)
        return web.json_response({"error": response.get("error", "Unable to remove asset")}, status=400)

    async def update_dependencies(request):
        graph_key = request.match_info["graph_key"]
        asset_key = request.match_info["asset_key"]
        body = await request.json()
        response = await asyncio.to_thread(
            request_graph,
            graph_key,
            {
                "type": "update_asset_dependencies",
                "asset_key": asset_key,
                "dependencies": body.get("dependencies", []),
            },
        )
        if response.get("ok"):
            return web.json_response(response)
        return web.json_response({"error": response.get("error", "Unable to update dependencies")}, status=400)

    async def update_properties(request):
        graph_key = request.match_info["graph_key"]
        asset_key = request.match_info["asset_key"]
        body = await request.json()
        response = await asyncio.to_thread(
            request_graph,
            graph_key,
            {
                "type": "update_asset_properties",
                "asset_key": asset_key,
                "properties": body.get("properties", {}),
            },
        )
        if response.get("ok"):
            return web.json_response(response)
        return web.json_response({"error": response.get("error", "Unable to update properties")}, status=400)

    async def add_dependency(request):
        graph_key = request.match_info["graph_key"]
        body = await request.json()
        response = await asyncio.to_thread(
            request_graph,
            graph_key,
            {
                "type": "add_dependency",
                "parent_key": body["parent_key"],
                "child_key": body["child_key"],
            },
        )
        if response.get("ok"):
            return web.json_response(response)
        return web.json_response({"error": response.get("error", "Unable to add dependency")}, status=400)

    app = web.Application()
    app.add_routes(
        [
            web.get("/graphs/{graph_key}", get_svg),
            web.get("/ui/{graph_key}", get_ui),
            web.get("/api/graphs/{graph_key}/snapshot", get_snapshot),
            web.get("/api/graphs/{graph_key}/assets/{asset_key}", get_asset_detail),
            web.post("/api/graphs/{graph_key}/assets/copy", copy_assets),
            web.post("/api/graphs/{graph_key}/assets/{asset_key}/build", trigger_build),
            web.post("/api/graphs/{graph_key}/assets/{asset_key}/refresh", trigger_refresh),
            web.post("/api/graphs/{graph_key}/assets", add_asset),
            web.delete("/api/graphs/{graph_key}/assets/{asset_key}", remove_asset),
            web.put("/api/graphs/{graph_key}/assets/{asset_key}/dependencies", update_dependencies),
            web.put("/api/graphs/{graph_key}/assets/{asset_key}/properties", update_properties),
            web.post("/api/graphs/{graph_key}/dependencies", add_dependency),
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
