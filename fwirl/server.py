import asyncio
import base64
import glob
import html
import json
import os
import pickle
import sys
import tempfile
from queue import Queue

import psutil
from aiohttp import web
from coolname import generate_slug
from loguru import logger

from .message import __RABBIT_URL__, get_msg, publish_msg
from .registry import list_running_graphs

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
      --detail-panel-width: 420px;
      --top-panel-height: 56vh;
    }}
    body {{
      margin: 0;
      font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, sans-serif;
      background: var(--bg);
      color: var(--text);
    }}
    .page {{
      display: grid;
      grid-template-rows: auto var(--top-panel-height) 8px minmax(220px, 1fr);
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
      grid-template-columns: minmax(520px, 1fr) 8px minmax(320px, var(--detail-panel-width));
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
    .resizer {{
      background: rgba(148, 163, 184, 0.25);
      border-radius: 999px;
      transition: background 0.15s ease;
    }}
    .resizer:hover {{
      background: rgba(56, 189, 248, 0.55);
    }}
    .resizer.vertical {{
      cursor: col-resize;
      min-height: 0;
    }}
    .resizer.horizontal {{
      cursor: row-resize;
      height: 8px;
    }}
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
    .actions {{ display: grid; grid-template-columns: repeat(2, 1fr); gap: 6px; }}
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
      <div id="vertical-resizer" class="resizer vertical"></div>
      <div class="panel">
        <div class="section-title">Asset Details</div>
        <div id="asset-detail" class="detail-grid"><div class="k">Selection</div><div>None</div></div>
        <div class="actions">
          <button id="build-btn">Build</button>
          <button id="refresh-btn">Refresh</button>
        </div>
        <div class="section-title">Properties</div>
        <details open>
          <summary>Edit selected asset properties (JSON)</summary>
          <div class="controls" style="margin-top:8px;">
            <textarea id="properties-editor" placeholder='{{"owner":"team-a"}}'></textarea>
            <button id="properties-save-btn">Save properties</button>
          </div>
        </details>
      </div>
    </div>
    <div id="horizontal-resizer" class="resizer horizontal"></div>
    <div class="panel">
      <div class="section-title">Assets</div>
      <div id="assets-table"></div>
    </div>
  </div>
  <script>
    const GRAPH_KEY = document.getElementById("graph-key").textContent;
    const pageEl = document.querySelector(".page");
    const layoutEl = document.querySelector(".layout");
    const graphWrap = document.getElementById("graph-wrap");
    const graphEl = document.getElementById("graph");
    const selectionBox = document.getElementById("selection-box");
    const verticalResizer = document.getElementById("vertical-resizer");
    const horizontalResizer = document.getElementById("horizontal-resizer");
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
    let renderedGraphSignature = null;
    let boxSelection = null;

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
        smooth: {{
          enabled: true,
          type: "cubicBezier",
          forceDirection: "vertical",
          roundness: 0.45
        }}
      }},
      layout: {{
        hierarchical: {{
          enabled: true,
          direction: "UD",
          sortMethod: "directed",
          levelSeparation: 120,
          nodeSpacing: 180,
          treeSpacing: 220,
          blockShifting: true,
          edgeMinimization: true,
          parentCentralization: true
        }}
      }},
      interaction: {{
        hover: true,
        navigationButtons: true,
        multiselect: true,
        dragNodes: false,
        dragView: false
      }},
      physics: false
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
        }} catch (error) {{
          alert(error.message);
          await fetchSnapshot();
        }}
      }}
    }});

    table.on("rowClick", (_, row) => {{
      const asset = row.getData();
      if (!asset || !asset.key) return;
      setSelectedAssets([asset.key]);
    }});

    network.on("click", (params) => {{
      if (boxSelection) return;
      if (params.nodes.length === 0) {{
        setSelectedAssets([]);
        return;
      }}
      const id = params.nodes[0];
      if (network.isCluster(id)) return;
      setSelectedAssets([id]);
    }});

    network.on("doubleClick", (params) => {{
      if (params.nodes.length === 0) return;
      const id = params.nodes[0];
      if (network.isCluster(id)) network.openCluster(id);
    }});

    graphWrap.addEventListener("mousedown", (event) => {{
      const rect = graphWrap.getBoundingClientRect();
      const point = {{ x: event.clientX - rect.left, y: event.clientY - rect.top }};
      const nodeId = network.getNodeAt(point);
      if (nodeId && !network.isCluster(nodeId)) {{
        return;
      }}
      boxSelection = {{ start: point, current: point }};
      drawSelectionBox();
    }});

    window.addEventListener("mousemove", (event) => {{
      const rect = graphWrap.getBoundingClientRect();
      const point = {{ x: event.clientX - rect.left, y: event.clientY - rect.top }};
      if (!boxSelection) return;
      boxSelection.current = point;
      drawSelectionBox();
    }});

    window.addEventListener("mouseup", (event) => {{
      const rect = graphWrap.getBoundingClientRect();
      const point = {{ x: event.clientX - rect.left, y: event.clientY - rect.top }};
      if (!boxSelection) return;
      boxSelection.current = point;
      const selected = nodesWithinSelection(boxSelection);
      boxSelection = null;
      selectionBox.style.display = "none";
      setSelectedAssets(selected);
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

    function escapeHtml(value) {{
      return String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
    }}

    function getSnapshotAsset(assetKey) {{
      return (latestSnapshot?.nodes || []).find((node) => node.key === assetKey) || null;
    }}

    function computeNodeLevels(snapshot) {{
      const nodes = snapshot?.nodes || [];
      const edges = snapshot?.edges || [];
      const childrenByNode = new Map(nodes.map((node) => [node.key, []]));
      const incomingCounts = new Map(nodes.map((node) => [node.key, 0]));
      const levels = new Map(nodes.map((node) => [node.key, 0]));

      for (const edge of edges) {{
        if (!childrenByNode.has(edge.from) || !incomingCounts.has(edge.to)) continue;
        childrenByNode.get(edge.from).push(edge.to);
        incomingCounts.set(edge.to, incomingCounts.get(edge.to) + 1);
      }}

      for (const children of childrenByNode.values()) {{
        children.sort();
      }}

      const queue = nodes
        .map((node) => node.key)
        .filter((key) => incomingCounts.get(key) === 0)
        .sort();
      const pending = [...queue];

      while (pending.length > 0) {{
        const key = pending.shift();
        const level = levels.get(key) || 0;
        for (const child of childrenByNode.get(key) || []) {{
          levels.set(child, Math.max(levels.get(child) || 0, level + 1));
          incomingCounts.set(child, incomingCounts.get(child) - 1);
          if (incomingCounts.get(child) === 0) {{
            pending.push(child);
            pending.sort();
          }}
        }}
      }}

      return levels;
    }}

    function graphSignature(snapshot) {{
      const nodeKeys = (snapshot.nodes || []).map((node) => node.key).sort();
      const edgeKeys = (snapshot.edges || [])
        .map((edge) => `${{edge.from}}->${{edge.to}}`)
        .sort();
      const collapseKeys = (snapshot.collapse_candidates || [])
        .map((candidate) => `${{candidate.id}}:${{(candidate.node_keys || []).slice().sort().join(",")}}`)
        .sort();
      return JSON.stringify({{ nodeKeys, edgeKeys, collapseKeys }});
    }}

    function replaceGraphData(nodes, edges, collapseCandidates) {{
      nodeData.clear();
      edgeData.clear();
      nodeData.add(nodes);
      edgeData.add(edges);

      for (const candidate of collapseCandidates || []) {{
        const clusterId = `cluster:${{candidate.id}}`;
        const nodeSet = new Set(candidate.node_keys || []);
        const clusterLevel = Math.min(
          ...(candidate.node_keys || []).map((key) => nodeData.get(key)?.level ?? 0)
        );
        network.cluster({{
          joinCondition: function(nodeOptions) {{ return nodeSet.has(nodeOptions.id); }},
          clusterNodeProperties: {{
            id: clusterId,
            label: `${{candidate.group ?? "ungrouped"}}/${{candidate.subgroup ?? "default"}} (${{candidate.status}})`,
            color: statusColors[candidate.status] || "#9ca3af",
            shape: "box",
            borderWidth: 1,
            level: Number.isFinite(clusterLevel) ? clusterLevel : 0
          }}
        }});
      }}
    }}

    function updateGraphData(nodes) {{
      nodeData.update(nodes);
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
        document.getElementById("properties-editor").value = "";
        return;
      }}
      document.getElementById("asset-detail").innerHTML = `
        <div class="k">Key</div><div>${{escapeHtml(asset.key)}}</div>
        <div class="k">Status</div><div>${{formatStatus(asset.status)}}</div>
        <div class="k">Type</div><div>${{escapeHtml(asset.type || "")}}</div>
        <div class="k">Group</div><div>${{escapeHtml(asset.group ?? "")}}</div>
        <div class="k">Subgroup</div><div>${{escapeHtml(asset.subgroup ?? "")}}</div>
        <div class="k">Last Build</div><div>${{escapeHtml(asset.last_build_timestamp ?? "N/A")}}</div>
        <div class="k">Timestamp</div><div>${{escapeHtml(asset.timestamp ?? "N/A")}}</div>
        <div class="k">Allow Retry</div><div>${{escapeHtml(asset.allow_retry ?? "")}}</div>
        <div class="k">Message</div><div>${{escapeHtml(asset.message || "")}}</div>
        <div class="k">Parents</div><div>${{escapeHtml((asset.parents || []).join(", "))}}</div>
        <div class="k">Children</div><div>${{escapeHtml((asset.children || []).join(", "))}}</div>
        <div class="k">Properties</div><pre>${{escapeHtml(JSON.stringify(asset.properties || {{}}, null, 2))}}</pre>
      `;
      document.getElementById("properties-editor").value = JSON.stringify(asset.properties || {{}}, null, 2);
    }}

    function parseApiResponse(text) {{
      if (!text) return null;
      try {{
        return JSON.parse(text);
      }} catch (_error) {{
        return text;
      }}
    }}

    async function api(path, opts={{}}) {{
      const res = await fetch(path, {{
        headers: {{ "Content-Type": "application/json" }},
        ...opts
      }});
      const text = await res.text();
      const data = parseApiResponse(text);
      if (!res.ok) {{
        throw new Error((data && typeof data === "object" ? data.error : null) || text || `HTTP ${{res.status}}`);
      }}
      if (data && typeof data === "string") {{
        throw new Error(`Expected JSON response from ${{path}}`);
      }}
      return data;
    }}

    async function fetchSnapshot() {{
      latestSnapshot = await api(`/api/graphs/${{encodeURIComponent(GRAPH_KEY)}}/snapshot`);
      const levels = computeNodeLevels(latestSnapshot);
      const nodes = latestSnapshot.nodes.map(n => ({{
        id: n.key,
        label: n.key,
        color: {{ background: statusColors[n.status] || "#9ca3af", border: "#0b1220" }},
        title: `${{n.key}} (${{n.status}})`,
        properties: n.properties || {{}},
        level: levels.get(n.key) || 0
      }}));
      const edges = latestSnapshot.edges.map(e => ({{
        id: `${{e.from}}->${{e.to}}`,
        from: e.from,
        to: e.to
      }}));
      const nextGraphSignature = graphSignature(latestSnapshot);
      if (nextGraphSignature !== renderedGraphSignature) {{
        replaceGraphData(nodes, edges, latestSnapshot.collapse_candidates || []);
        renderedGraphSignature = nextGraphSignature;
      }} else {{
        updateGraphData(nodes);
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
      renderDetails(selectedAsset ? getSnapshotAsset(selectedAsset) : null);
    }}

    async function saveProperties(assetKey, properties) {{
      await api(`/api/graphs/${{encodeURIComponent(GRAPH_KEY)}}/assets/${{encodeURIComponent(assetKey)}}/properties`, {{
        method: "PUT",
        body: JSON.stringify({{ properties }})
      }});
    }}

    function setSelectedAssets(assetKeys) {{
      selectedAssets = [...new Set(assetKeys)];
      selectedAsset = selectedAssets.length > 0 ? selectedAssets[0] : null;
      updateSelectionStyles();
      renderDetails(selectedAsset ? getSnapshotAsset(selectedAsset) : null);
    }}

    async function triggerAction(action) {{
      if (!selectedAsset) return;
      await api(`/api/graphs/${{encodeURIComponent(GRAPH_KEY)}}/assets/${{encodeURIComponent(selectedAsset)}}/${{action}}`, {{
        method: "POST"
      }});
      await fetchSnapshot();
    }}

    document.getElementById("build-btn").addEventListener("click", () => triggerAction("build"));
    document.getElementById("refresh-btn").addEventListener("click", () => triggerAction("refresh"));

    document.getElementById("properties-save-btn").addEventListener("click", async () => {{
      if (!selectedAsset) return;
      const properties = parseJsonField(document.getElementById("properties-editor").value, {{}});
      await saveProperties(selectedAsset, properties);
      await fetchSnapshot();
    }});

    function installResizer(handle, onMove) {{
      let dragging = false;
      handle.addEventListener("mousedown", (event) => {{
        event.preventDefault();
        dragging = true;
        document.body.style.userSelect = "none";
      }});
      window.addEventListener("mousemove", (event) => {{
        if (!dragging) return;
        onMove(event);
      }});
      window.addEventListener("mouseup", () => {{
        if (!dragging) return;
        dragging = false;
        document.body.style.userSelect = "";
      }});
    }}

    installResizer(verticalResizer, (event) => {{
      const rect = layoutEl.getBoundingClientRect();
      const width = rect.right - event.clientX;
      const clamped = Math.max(320, Math.min(760, width));
      document.documentElement.style.setProperty("--detail-panel-width", `${{clamped}}px`);
    }});

    installResizer(horizontalResizer, (event) => {{
      const layoutRect = layoutEl.getBoundingClientRect();
      const pageRect = pageEl.getBoundingClientRect();
      const maxHeight = pageRect.bottom - layoutRect.top - horizontalResizer.offsetHeight - 220;
      const height = event.clientY - layoutRect.top;
      const clamped = Math.max(280, Math.min(maxHeight, height));
      document.documentElement.style.setProperty("--top-panel-height", `${{clamped}}px`);
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
  <script>
    localStorage.setItem("fwirl:lastGraphKey", {json.dumps(graph_key)});
  </script>
</body>
</html>"""


def landing_html():
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>fwirl dashboard</title>
  <style>
    :root {
      --bg: #0f172a;
      --panel: #1e293b;
      --text: #e2e8f0;
      --muted: #94a3b8;
      --accent: #38bdf8;
    }
    body {
      margin: 0;
      min-height: 100vh;
      display: grid;
      place-items: center;
      background: var(--bg);
      color: var(--text);
      font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, sans-serif;
    }
    .card {
      width: min(460px, calc(100vw - 32px));
      background: var(--panel);
      border-radius: 16px;
      padding: 24px;
      box-sizing: border-box;
    }
    h1 { margin: 0 0 10px; font-size: 24px; }
    p { color: var(--muted); line-height: 1.5; }
    form { display: grid; grid-template-columns: 1fr auto; gap: 10px; margin-top: 18px; }
    select, button {
      border: 1px solid rgba(148, 163, 184, 0.35);
      border-radius: 10px;
      padding: 10px 12px;
      font: inherit;
    }
    select {
      background: rgba(15, 23, 42, 0.75);
      color: var(--text);
    }
    button {
      background: var(--accent);
      color: #082f49;
      cursor: pointer;
      font-weight: 600;
    }
    button:disabled {
      opacity: 0.6;
      cursor: not-allowed;
    }
    code {
      background: rgba(15, 23, 42, 0.75);
      border-radius: 6px;
      padding: 2px 6px;
    }
  </style>
</head>
<body>
  <main class="card">
    <h1>fwirl dashboard</h1>
    <p id="graph-status">Choose a running graph to open its dashboard. The bundled examples use <code>test_graph</code>.</p>
    <form id="graph-form">
      <select id="graph-key" name="graph" disabled>
        <option>Loading graphs...</option>
      </select>
      <button type="submit" id="open-button" disabled>Open</button>
    </form>
  </main>
  <script>
    const input = document.getElementById("graph-key");
    const status = document.getElementById("graph-status");
    const openButton = document.getElementById("open-button");
    const requested = new URLSearchParams(window.location.search).get("graph");
    const remembered = localStorage.getItem("fwirl:lastGraphKey");
    async function loadGraphs() {
      try {
        const response = await fetch("/api/graphs");
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        const payload = await response.json();
        const graphs = Array.isArray(payload.graphs) ? payload.graphs : [];
        input.innerHTML = "";
        if (graphs.length === 0) {
          input.disabled = true;
          openButton.disabled = true;
          input.innerHTML = '<option value="">No running graphs available</option>';
          status.textContent = "No running graphs found. Start a graph and refresh this page.";
          return;
        }
        for (const graph of graphs) {
          const option = document.createElement("option");
          option.value = graph;
          option.textContent = graph;
          input.appendChild(option);
        }
        const preferred = [requested, remembered].find((key) => key && graphs.includes(key)) || graphs[0];
        input.value = preferred;
        input.disabled = false;
        openButton.disabled = false;
        status.textContent = `${graphs.length} running graph${graphs.length === 1 ? "" : "s"} available.`;
      } catch (error) {
        input.disabled = true;
        openButton.disabled = true;
        input.innerHTML = '<option value="">Unable to load running graphs</option>';
        status.textContent = `Unable to load running graphs: ${error.message}`;
      }
    }
    loadGraphs();
    document.getElementById("graph-form").addEventListener("submit", (event) => {
      event.preventDefault();
      const key = input.value.trim();
      if (key) {
        window.location.href = `/ui/${encodeURIComponent(key)}`;
      }
    });
  </script>
</body>
</html>"""


def aiohttp_server():
    async def get_root(request):
        return web.Response(text=landing_html(), content_type="text/html")

    async def get_graphs(request):
        return web.json_response({"graphs": list_running_graphs()})

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

    app = web.Application()
    app.add_routes(
        [
            web.get("/", get_root),
            web.get("/api/graphs", get_graphs),
            web.get("/graphs/{graph_key}", get_svg),
            web.get("/ui/{graph_key}", get_ui),
            web.get("/api/graphs/{graph_key}/snapshot", get_snapshot),
            web.get("/api/graphs/{graph_key}/assets/{asset_key}", get_asset_detail),
            web.post("/api/graphs/{graph_key}/assets/{asset_key}/build", trigger_build),
            web.post("/api/graphs/{graph_key}/assets/{asset_key}/refresh", trigger_refresh),
            web.put("/api/graphs/{graph_key}/assets/{asset_key}/properties", update_properties),
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
    run_server(aiohttp_server())


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
