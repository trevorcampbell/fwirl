import networkx as nx

class AssetGraph:
    def __init__(self):
        self.graph = nx.DiGraph()
        self.assets = {}

        nx.is_directed_acyclic_graph(graph)

        downstream = [n for n in nx.traversal.bfs_tree(g, 2) if n != 2]

        graph.remove_nodes_from(iterable_nodes_e.g._bfs_tree)

        upstream = [n for n in nx.traversal.bfs_tree(g, 2, reverse=True) if n != 2]

        graph.add_edge(source_node, dest_node) #also adds node
        graph.add_edges_from(edges) #also adds nodes

        graph.nodes[asset_key]

    def add_asset(self, asset):
        graph.add_node(asset.key)
        assets[asset.key] = asset

    def propagate_status(self):
        # Status propagates using the following cascade of rules:
        # 1. If any of my parents is Paused or UpstreamPaused, I'm UpstreamPaused.
        # 2. If I don't exist, I'm Unavailable.
        # 4. If any of my parents is Stale or Failed or Unavailable, I'm Stale
        # 5. All of my parents are Current. So check timestamps. If my timestamp is earlier than any of them, I'm Stale.
        # 6. I'm Current
        for asset in nx.topological_sort(self.graph):
            any_paused = False
            any_stale_failed_un = False
            latest_parent_timestamp = None
            for parent in self.graph.predecessors(asset):
                if parent.status == AssetStatus.Paused or parent.status == AssetStatus.UpstreamPaused:
                    any_paused = True
                if parent.status == AssetStatus.Stale or parent.status == AssetStatus.Failed or parent.status == AssetStatus.Unavailable:
                    any_stale_failed_un = True
                # don't try to compute timestamps if any parent is paused, since they may not exist (and have no cached ts)
                if parent.status != AssetStatus.Unavailable and (not any_paused):
                    ts = parent._cached_timestamp
                    latest_parent_timestamp = latest_parent_timestamp if latest_parent_timestamp > ts else ts

            if any_paused:
                asset.status = AssetStatus.UpstreamPaused
                continue

            timestamp = asset._timestamp()
            if timestamp is None:
                asset.status = AssetStatus.Unavailable
                continue

            if any_stale_failed_un or timestamp < latest_parent_timestamp:
                asset.status = AssetStatus.Stale
                continue

            asset.status = AssetStatus.Current
        return

    def build(self, root=None):
        # Builds only happen for Unavailable and Stale assets whose parents are all Current
        for asset in nx.topological_sort(self.graph if root is None else self.graph[root]):
            if (asset.status == AssetStatus.Unavailable or asset.status == AssetStatus.Stale) and all(p.status == AssetStatus.Current for p in nx.predecessors(asset)):
                # set building status
                asset.status = AssetStatus.Building

                # run the build
                asset.build()

                # refresh the timestamp cache
                ts = asset._timestamp()

                # make sure asset is now available and has a new timestamp
                if ts is AssetStatus.Unavailable:
                    asset.status = AssetStatus.Failed
                    asset.message = "Build failed: asset unavailable"
                    continue

                # ensure that it now has a newer timestamp than parents
                if not all(p._cached_timestamp < asset._cached_timestamp for p in nx.predecessors(asset)):
                    asset.status = AssetStatus.Failed
                    asset.message = "Build failed: asset timestamp older than parents"
                    continue

                # update to current status
                asset.status = AssetStatus.Current
                asset.message = "Build complete"
        return



