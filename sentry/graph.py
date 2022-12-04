import networkx as nx
from loguru import logger
from .asset import AssetStatus
import matplotlib.pyplot as plt
from collections import Counter, defaultdict
#import notifiers

_NODE_COLORS = {AssetStatus.Current : "tab:green",
                AssetStatus.Stale : "khaki",
                AssetStatus.Building : "tab:blue",
                AssetStatus.Paused : "slategray",
                AssetStatus.UpstreamPaused : "lightslategray",
                AssetStatus.Unavailable : "gray",
                AssetStatus.Failed : "tab:red"
            }

# TODO: use HEX to match above colors <fg #00005f>, <fg #EE1>	
_LOGURU_COLORS = {AssetStatus.Current : "32",
                AssetStatus.Stale : "33",
                AssetStatus.Building : "34",
                AssetStatus.Paused : "35",
                AssetStatus.UpstreamPaused : "35",
                AssetStatus.Unavailable : "37",
                AssetStatus.Failed : "91"
            }

def fmt(status):
    #return f"<{_LOGURU_COLORS[status]}>{status}</>"
    return f"\033[{_LOGURU_COLORS[status]}m{status}\033[00m\u001b[1m"

class AssetGraph:
    def __init__(self):
        self.graph = nx.DiGraph()
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
        self._stale_topological_sort = True
        logger.info(f"Added {new_edges-old_edges} unique edges and {new_nodes-old_nodes} unique assets to the graph")

    def remove_assets(self, assets):
        logger.info(f"Removing {len(assets)} assets and downstream assets from graph")
        old_edges = self.graph.size()
        old_nodes = self.graph.number_of_nodes()
        self.graph.remove_nodes_from(assets)
        new_edges = self.graph.size()
        new_nodes = self.graph.number_of_nodes()
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
                logger.info(f"Asset {asset} status: {fmt(asset.status)}")
                continue

            timestamp = asset._timestamp()
            if timestamp == AssetStatus.Unavailable:
                asset.status = AssetStatus.Unavailable
                logger.info(f"Asset {asset} status: {fmt(asset.status)}")
                continue

            if any_stale_failed_un or (timestamp < latest_parent_timestamp):
                asset.status = AssetStatus.Stale
                logger.info(f"Asset {asset} status: {fmt(asset.status)}")
                continue

            asset.status = AssetStatus.Current
            logger.info(f"Asset {asset} status: {fmt(asset.status)}")
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
            logger.debug(f"Checking asset {asset} with status {asset.status} and parent statuses {(' '.join([str(item[0]) +': ' + str(item[1]) + ',' for item in list(parent_status_cts.items())]))[:-1]}")
            if (asset.status == AssetStatus.Unavailable or asset.status == AssetStatus.Stale) and all(p.status == AssetStatus.Current for p in self.graph.predecessors(asset)):
                logger.info(f"Asset {asset} is {fmt(asset.status)} and all parents are current; rebuilding")
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

                # get latest parent timestamp
                parents = list(self.graph.predecessors(asset))
                if len(parents) == 0:
                    logger.debug(f"Latest parent asset timestamp: (no parents)")
                else:
                    latest_parent_timestamp = max([p._cached_timestamp for p in parents])
                    logger.debug(f"Latest parent asset timestamp: {latest_parent_timestamp}")
                    if ts < latest_parent_timestamp:
                        raise RuntimeError("Rebuilt asset has timestamp earlier than latest parent!")

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
                logger.info(f"Asset {asset} build completed successfully (status: {fmt(asset.status)})")
            else:
                logger.debug(f"Asset {asset} not ready to rebuild.")
        return

    def _collect_groups(self):
        logger.debug("Collecting node (sub)groups")
        groupings = defaultdict(list)
        for asset in self.graph:
            if asset.group == '__singletons__' or asset.subgroup == '__singletons__':
                raise ValueError('(Sub)group name used reserved keyword __singletons__')
            if asset.group is not None:
                if asset.group not in groupings:
                    groupings[asset.group] = defaultdict(list)
                if asset.subgroup is not None:
                    groupings[asset.group][asset.subgroup].append(asset)
                else:
                    groupings[asset.group]['__singletons__'].append(asset)
            else:
                groupings['__singletons__'].append(asset) 
        # topologically sort the subgraphs
        logger.debug("Topologically sorting subgroups")
        for gr in groupings:
            if gr == '__singletons__':
                sg = self.graph.subgraph(groupings[gr])
                groupings[gr] = list(nx.topological_sort(sg))
                continue
            for sb in groupings[gr]:
                sg = self.graph.subgraph(groupings[gr][sb])
                groupings[gr][sb] = list(nx.topological_sort(sg))
        return groupings
    
    def summarize(self):
        groupings = self._collect_groups()
        summary = f"\nAsset Graph Summary\n-------------------\nAssets: {self.graph.number_of_nodes()}\n"
        for status in AssetStatus:
            summary += f"    {fmt(status)}: {sum([a.status == status for a in self.graph])} assets\n" 
        summary += f"Edges: {self.graph.size()}\n"
        summary += f"Asset Groups: {len(groupings) - 1} groups, {len(groupings['__singletons__'])} singletons\n"
        for gr in groupings:
            # report individuals 
            if gr == '__singletons__':
                # report summarized individuals
                status_groups = defaultdict(int)
                for asset in groupings[gr]:
                    if asset.status != AssetStatus.Failed:
                        status_groups[asset.status] += 1 
                summary += f"    Singletons:\n"
                for status in status_groups:
                    summary += f"        {fmt(status)}: {status_groups[status]} singletons\n"
                # report special cases
                for asset in groupings[gr]:
                    if asset.status == AssetStatus.Failed:
                        summary += f"        {asset}: {fmt(asset.status)}\n"
                continue
            summary += f"    Group {gr}: {len(groupings[gr]) - 1} subgroups, {len(groupings[gr]['__singletons__'])} singletons\n"
            # collect and report the summarized subgroups
            status_groups = defaultdict(int)
            for sb in groupings[gr]:
                if sb == '__singletons__':
                    for asset in groupings[gr][sb]:
                        if asset.status != AssetStatus.Failed:
                            status_groups[asset.status] += 1
                elif len({asset.status for asset in groupings[gr][sb]}) == 1 and groupings[gr][sb][0].status != AssetStatus.Failed:
                    status_groups[groupings[gr][sb][0].status] += 1
            for sg in status_groups:
                summary += f"        {sg}: {status_groups[sg]} subgroups/singletons\n"
            # collect and report special cases
            for sb in groupings[gr]:
                if sb == '__singletons__':
                    for asset in groupings[gr][sb]:
                        if asset.status == AssetStatus.Failed:
                            summary += f"        Singleton {asset}: {fmt(asset.status)}\n"
                elif len({asset.status for asset in groupings[gr][sb]}) > 1 or groupings[gr][sb][0].status == AssetStatus.Failed:
                    summary += f"        Subgroup {sb}:\n"
                    for asset in groupings[gr][sb]:
                        summary += f"            {asset} : {fmt(asset.status)}\n"
        logger.info(summary, colorize=True)
        

    def visualize(self):
        # in order to visualize large graphs
        # collapse all subgroups within a group with all constant state 
        logger.info("Visualizing asset graph") 
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
        vizgraph = nx.DiGraph()
        # add all viznodes
        for asset in self.graph:
            vizgraph.add_edges_from([(p.viznode, asset.viznode) for p in self.graph.predecessors(asset)]) 
        # remove any self-edges caused by viznode
        for node in vizgraph:
            try:	
                vizgraph.remove_edge(node, node)
            except:
                pass
        logger.info(f"Quotient graph has {vizgraph.number_of_nodes()} nodes and {vizgraph.size()} edges.")
        logger.info("Computing quotient node positions")
        pos = nx.planar_layout(vizgraph)
        pos = nx.spring_layout(vizgraph, pos=pos) #initialize spring with planar
        logger.info("Drawing the graph")
        node_colors = [_NODE_COLORS[node[2]] for node in vizgraph]
        node_sizes = [600 if node[0] == "group" else 100 for node in vizgraph]
        nx.draw(vizgraph, pos=pos, node_color=node_colors, node_size=node_sizes)
        plt.show()

        





