import networkx as nx
from loguru import logger
from .asset import AssetStatus
import matplotlib.pyplot as plt
from collections import Counter
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
        self.vizgraph = None
        self.pos = None
        self._stale_viz = True
        self._stale_topological_sort = True
        self._cached_topological_sort = None

    def add_assets(self, assets):
        logger.info(f"Gathering edges, assets, and upstream assets to add to the graph")
        edges = []
        unq_assets = set()
        asset_queue = []
        asset_queue.extend(assets)
        while len(asset_queue) != 0:
            a = asset_queue.pop()
            if a in a.dependencies:
                # TODO test this check...
                raise ValueError("Self-dependency loop detected!")
            if a in unq_assets or a in self.graph:
                continue
            unq_assets.add(a)
            edges.extend([(d, a) for d in a.dependencies])
            asset_queue.extend(a.dependencies)
        logger.info(f"Adding {len(edges)} edges, {len(assets)} assets, and upstream assets to the graph")
        old_edges = self.graph.size()
        old_nodes = self.graph.number_of_nodes()
        self.graph.add_edges_from(edges)
        new_edges = self.graph.size()
        new_nodes = self.graph.number_of_nodes()
        self._stale_viz = True
        self._stale_topological_sort = True
        logger.info(f"Added {new_edges-old_edges} unique edges and {new_nodes-old_nodes} unique assets to the graph")

    def remove_assets(self, assets):
        logger.info(f"Removing {len(assets)} assets and downstream assets from graph")
        old_edges = self.graph.size()
        old_nodes = self.graph.number_of_nodes()
        self.graph.remove_nodes_from(assets)
        new_edges = self.graph.size()
        new_nodes = self.graph.number_of_nodes()
        self._stale_viz = True
        self._stale_topological_sort = True
        logger.info(f"Removed {old_nodes - new_nodes} assets and {old_edges - new_edges} edges from the graph")

    def propagate_status(self):
        # Status propagates using the following cascade of rules:
        # 1. If any of my parents is Paused or UpstreamPaused, I'm UpstreamPaused.
        # 2. If I don't exist, I'm Unavailable.
        # 4. If any of my parents is Stale or Failed or Unavailable, I'm Stale
        # 5. All of my parents are Current. So check timestamps. If my timestamp is earlier than any of them, I'm Stale.
        # 6. I'm Current
        logger.info(f"Running status propagation on asset graph")
        # refresh the topological sort if needed
        if self._stale_topological_sort:
            logger.info(f"Asset graph structure changed; recomputing the topological sort")
            self._cached_topological_sort = list(nx.topological_sort(self.graph))
            self._stale_topological_sort = False
        for asset in self._cached_topological_sort:
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

    def build(self):
        # Builds only happen for Unavailable and Stale assets whose parents are all Current
        logger.info(f"Running build on asset graph")
        if self._stale_topological_sort:
            logger.info(f"Asset graph structure changed; recomputing the topological sort")
            self._cached_topological_sort = list(nx.topological_sort(self.graph))
            self._stale_topological_sort = False
        for asset in self._cached_topological_sort:
            parent_status_cts = Counter([p.status for p in self.graph.predecessors(asset)])
            logger.debug(f"Checking asset {asset} with status {asset.status} and parent statuses {list(parent_status_cts.items())}")
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
                    continue

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
            else:
                logger.debug(f"Asset {asset} not ready to rebuild.")
        return

    def visualize(self):
        # in order to visualize large graphs
        # collapse all subgroups within a group with all constant state 
        logger.info("Visualizing asset graph") 
        # TODO can't cache vizgraph since structure changes whenever status changes...
        if self._stale_viz:
            logger.info("Asset graph structure changed; recomputing quotient graph structure and node positions")
            groupings = {}
            logger.info("Assembling viznodes")
            for asset in self.graph:
                # if the asset is part of a group,subgroup
                if (asset.group is not None) and (asset.subgroup is not None):
                    # collect assets in each group/subgroup
                    if asset.group not in groupings:
                        groupings[asset.group] = {}
                    if asset.subgroup not in groupings[asset.group]:
                        groupings[asset.group][asset.subgroup] = []
                    groupings[asset.group][asset.subgroup].append(asset)
                else:
                    #ungrouped assets get their own viznodes
                    asset.viznode = ("single",asset.hash,asset.status)
            # for grouped assets, merge into one viznode if status is the same across the subgroup
            for group in groupings:
                for subgroup in groupings[group]:
                    if len({asset.status for asset in groupings[group][subgroup]}) == 1:
                        for asset in groupings[group][subgroup]:
                            asset.viznode = ("group",group,asset.status)
                    else:
                        for asset in groupings[group][subgroup]:
                            asset.viznode = ("single",asset.hash,asset.status)
            logger.info("Building vizgraph")
            # create the vizgraph
            self.vizgraph = nx.DiGraph()
            # add all viznodes
            for asset in self.graph:
                self.vizgraph.add_edges_from([(p.viznode, asset.viznode) for p in self.graph.predecessors(asset)]) 
            # remove any self-edges caused by viznode
            for node in self.vizgraph:
                try:	
                    self.vizgraph.remove_edge(node, node)
                except:
                    pass
            print([node for node in self.vizgraph])
            logger.info(f"Quotient graph has {self.vizgraph.number_of_nodes()} nodes and {self.vizgraph.size()} edges.")
            logger.info("Computing quotient node positions")
            self.pos = nx.kamada_kawai_layout(self.vizgraph)
            self.pos = nx.spring_layout(self.vizgraph, pos=self.pos)
            self._stale_viz = False
        logger.info("Drawing the graph")
        #node_colors = [_NODE_COLORS[asset.status] for asset in self.graph]
        node_colors = [_NODE_COLORS[node[2]] for node in self.vizgraph]
        node_sizes = [600 if node[0] == "group" else 100 for node in self.vizgraph]
        nx.draw(self.vizgraph, pos=self.pos, node_color=node_colors, node_size=node_sizes)
        plt.show()





