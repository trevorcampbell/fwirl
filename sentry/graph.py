import networkx as nx
from loguru import logger
from .asset import AssetStatus
import matplotlib.pyplot as plt
#import notifiers

_NODE_COLORS = {AssetStatus.Current : "tab:green",
                AssetStatus.Stale : "khaki",
                AssetStatus.Building : "tab:blue",
                AssetStatus.Paused : "slategray",
                AssetStatus.UpstreamPaused : "lightslategray",
                AssetStatus.Unavailable : "gray",
                AssetStatus.Failed : "tab:red"
            }

class AssetGraph:
    def __init__(self):
        self.graph = nx.DiGraph()
        self.pos = None
        self.stale_node_positions = False

        #nx.is_directed_acyclic_graph(graph)
        #downstream = [n for n in nx.traversal.bfs_tree(g, 2) if n != 2]
        #graph.remove_nodes_from(iterable_nodes_e.g._bfs_tree)
        #upstream = [n for n in nx.traversal.bfs_tree(g, 2, reverse=True) if n != 2]
        #graph.add_edge(source_node, dest_node) #also adds node
        #graph.add_edges_from(edges) #also adds nodes
        #graph.nodes[asset_key]

    def add_assets(self, assets):
        for a in assets:
            edges = [(d, a) for d in a.dependencies]
            logger.info(f"Added edges {edges} to asset graph")
            self.graph.add_edges_from([(d, a) for d in a.dependencies])
        self.stale_node_positions = True

    def remove_assets(self, assets):
        logger.info(f"Removed assets {assets} from asset graph")
        self.graph.remove_nodes_from(assets)
        self.stale_node_positions = True

    def propagate_status(self):
        # Status propagates using the following cascade of rules:
        # 1. If any of my parents is Paused or UpstreamPaused, I'm UpstreamPaused.
        # 2. If I don't exist, I'm Unavailable.
        # 4. If any of my parents is Stale or Failed or Unavailable, I'm Stale
        # 5. All of my parents are Current. So check timestamps. If my timestamp is earlier than any of them, I'm Stale.
        # 6. I'm Current
        logger.info(f"Running status propagation on asset graph")
        for asset in nx.topological_sort(self.graph):
            logger.debug(f"Updating status for asset {asset}")
            any_paused = False
            any_stale_failed_un = False
            latest_parent_timestamp = AssetStatus.Unavailable
            for parent in self.graph.predecessors(asset):
                logger.debug(f"Checking parent {parent} with status {parent.status} and timestamp {parent._cached_timestamp}")
                if parent.status == AssetStatus.Paused or parent.status == AssetStatus.UpstreamPaused:
                    any_paused = True
                if parent.status == AssetStatus.Stale or parent.status == AssetStatus.Failed or parent.status == AssetStatus.Unavailable:
                    any_stale_failed_un = True
                # don't try to compute timestamps if any parent is paused, since they may not exist (and have no cached ts)
                if parent.status != AssetStatus.Unavailable and (not any_paused):
                    ts = parent._cached_timestamp
                    if latest_parent_timestamp == AssetStatus.Unavailable:
                        latest_parent_timestamp = ts
                    else:
                        latest_parent_timestamp = latest_parent_timestamp if latest_parent_timestamp > ts else ts
                logger.debug(f"After processing parent {parent}: any_paused = {any_paused}, any_stale_failed_un = {any_stale_failed_un}, latest_ts = {latest_parent_timestamp}")

            if any_paused:
                asset.status = AssetStatus.UpstreamPaused
                logger.info(f"Asset {asset} status: {asset.status}")
                continue

            timestamp = asset._timestamp()
            if timestamp == AssetStatus.Unavailable:
                asset.status = AssetStatus.Unavailable
                logger.info(f"Asset {asset} status: {asset.status}")
                continue

            if any_stale_failed_un or (timestamp < latest_parent_timestamp):
                asset.status = AssetStatus.Stale
                logger.info(f"Asset {asset} status: {asset.status}")
                continue

            asset.status = AssetStatus.Current
            logger.info(f"Asset {asset} status: {asset.status}")
        return

    def build(self, root=None):
        # Builds only happen for Unavailable and Stale assets whose parents are all Current
        logger.info(f"Running build on asset graph")
        for asset in nx.topological_sort(self.graph if root is None else self.graph[root]):
            logger.debug(f"Checking asset {asset} with status {asset.status} and parent statuses {[p.status for p in self.graph.predecessors(asset)]}")
            if (asset.status == AssetStatus.Unavailable or asset.status == AssetStatus.Stale) and all(p.status == AssetStatus.Current for p in self.graph.predecessors(asset)):
                logger.info(f"Asset {asset} is {asset.status} and all parents are current; rebuilding")
                # set building status
                asset.status = AssetStatus.Building

                # run the build
                try:
                    asset.build()
                except Exception as e:
                    asset.status = AssetStatus.Failed
                    asset.message = "Build failed: exception during build"
                    logger.exception(f"Asset {asset} build failed (status: {asset.status}): error in-build")

                # refresh the timestamp cache
                ts = asset._timestamp()

                logger.debug(f"New asset {asset} timestamp: {ts}")
                logger.debug(f"Parent asset timestamps: {[str(p._cached_timestamp) for p in self.graph.predecessors(asset)]}")

                # make sure asset is now available and has a new timestamp
                if ts is AssetStatus.Unavailable:
                    asset.status = AssetStatus.Failed
                    asset.message = "Build failed: asset unavailable"
                    logger.error(f"Asset {asset} build failed (status: {asset.status}): asset unavailable after build")
                    continue

                # ensure that it now has a newer timestamp than parents
                if not all(p._cached_timestamp < asset._cached_timestamp for p in self.graph.predecessors(asset)):
                    asset.status = AssetStatus.Failed
                    asset.message = "Build failed: asset timestamp older than parents"
                    logger.error(f"Asset {asset} build failed (status: {asset.status}): asset timestamp older than parents")
                    continue

                # update to current status
                asset.status = AssetStatus.Current
                asset.message = "Build complete"
                logger.info(f"Asset {asset} build completed successfully (status: {asset.status})")
        return

    def visualize(self):
        if self.stale_node_positions:
            #self.pos = nx.multipartite_layout(self.graph)
            self.pos = nx.planar_layout(self.graph)
        node_colors = [_NODE_COLORS[asset.status] for asset in self.graph]
        nx.draw(self.graph, pos=self.pos, node_color=node_colors)
        plt.show()




