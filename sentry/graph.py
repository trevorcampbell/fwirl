import networkx as nx
from loguru import logger
from .asset import AssetStatus
import matplotlib.pyplot as plt
from collections import Counter, defaultdict
from collections.abc import Iterable
import pendulum as plm
#import notifiers
from kombu import Connection, Exchange, Queue
from .schedule import Schedule

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

# TODO: use better formatting than ANSI...
def fmt(status):
    #return f"<{_LOGURU_COLORS[status]}>{status}</>"
    return f"\033[{_LOGURU_COLORS[status]}m{status}\033[00m\u001b[1m"

class AssetGraph:
    def __init__(self, key, rabbit_url = "amqp://guest:guest@localhost//"):
        self.graph = nx.DiGraph()
        self.key = key
        self.schedules = {}
        self.rabbit_url = rabbit_url
        #self._stale_topological_sort = True
        #self._cached_topological_sort = None

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
        #self._stale_topological_sort = True
        logger.info(f"Added {new_edges-old_edges} unique edges and {new_nodes-old_nodes} unique assets to the graph")

    def remove_assets(self, assets):
        logger.info(f"Removing {len(assets)} assets and downstream assets from graph")
        old_edges = self.graph.size()
        old_nodes = self.graph.number_of_nodes()
        self.graph.remove_nodes_from(assets)
        new_edges = self.graph.size()
        new_nodes = self.graph.number_of_nodes()
        #self._stale_topological_sort = True
        logger.info(f"Removed {old_nodes - new_nodes} assets and {old_edges - new_edges} edges from the graph")

    def schedule(self, schedule_key, cron_string, asset = None):
        jobstr = "build all" if asset is None else f"build upstream of {asset}"
        logger.info(f"Adding scheduled run '{schedule_key}' ({jobstring} at '{cron_string}') to graph {self.key}")
        self.schedules[schedule_key] = Schedule(cron_string, asset)

    def pause_schedule(self, schedule_key):
        self.schedules[schedule_key].pause()

    def unpause_schedule(self, schedule_key):
        self.schedules[schedule_key].unpause()

    def on_message(self, body, message):
        print(f"received message: {body}")
        message.ack()
        if body == "summarize":
            self.summarize()

    def run(self):
        # run the message handling loop
        logger.info(f"Beginning sentry main loop")
        exchange = Exchange('sentry', 'direct', durable = False)
        queue = Queue(self.key, exchange=exchange, routing_key = self.key)
        with Connection(self.rabbit_url) as conn:
            with conn.Consumer(rabbit_queue, callbacks=[self.on_message]):
                while True:
                    # get next earliest event from the schedules
                    min_wait = None
                    next_sk = None
                    for sk in self.schedules:
                        if not self.schedules[sk].is_paused():
                            wait = self.schedules[sk].next()
                            if min_wait is None or min_wait > wait:
                                min_wait = wait
                                next_sched = sk
                    if min_wait is None:
                        logger.info(f"Waiting on message queue (no timeout)")
                    else:
                        logger.info(f"Waiting on message queue and next run of {self.schedules[next_sk]} at {plm.now() + plm.duration(seconds=min_wait)}")
                    try:
                        conn.drain_events(timeout = min_wait)
                    except KeyboardInterrupt:
                        logger.info(f"Caught keyboard interrupt; stopping event loop of asset graph {self.key}")
                        break
                    # do something with next_sched
                    logger.info(f"Running scheduled event {next_sk}")
                    # TODO run the event


    def refresh_status(self):
        # Status propagates using the following cascade of rules:
        # 0. If I'm Failed and self.allow_retry = False, I'm Failed
        # 1. If any of my parents is Paused or UpstreamPaused or Failed, I'm UpstreamPaused.
        # 2. If I don't exist, I'm Unavailable.
        # 3. If any of my parents is Stale or Unavailable, I'm Stale
        # 4. All of my parents are Current. So check timestamps. If my timestamp is earlier than any of them, I'm Stale.
        # 5. I'm Current
        logger.info(f"Running status refresh & propagation on asset graph")
        sorted_nodes = list(nx.topological_sort(self.graph))

        required_resources = set()
        for asset in sorted_nodes:
            required_resources.update(asset.resources)
        if not self._initialize_resources(required_resources):
            return
        ## refresh the topological sort if needed
        #if self._stale_topological_sort:
        #    logger.info(f"Asset graph structure changed; recomputing the topological sort")
        #    self._cached_topological_sort = list(nx.topological_sort(self.graph))
        #    self._stale_topological_sort = False
        for asset in sorted_nodes:
            logger.debug(f"Updating status for asset {asset}")
            if (not asset.allow_retry) and (asset.status == AssetStatus.Failed):
                logger.info(f"Asset {asset} status: {fmt(asset.status)} (previous failure and asset does not allow retries)")
                continue
            any_paused_failed = False
            any_stale_unav = False
            latest_parent_timestamp = AssetStatus.Unavailable
            for parent in self.graph.predecessors(asset):
                logger.debug(f"Checking parent {parent} with status {parent.status} and timestamp {parent._cached_timestamp}")
                if parent.status == AssetStatus.Paused or parent.status == AssetStatus.UpstreamPaused or parent.status == AssetStatus.Failed:
                    any_paused_failed = True
                if parent.status == AssetStatus.Stale or parent.status == AssetStatus.Unavailable:
                    any_stale_unav = True
                # don't try to compute timestamps if any parent is paused, since they may not exist (and have no cached ts)
                if parent.status != AssetStatus.Unavailable and (not any_paused_failed):
                    ts = parent._cached_timestamp
                    if latest_parent_timestamp == AssetStatus.Unavailable:
                        latest_parent_timestamp = ts
                    else:
                        latest_parent_timestamp = latest_parent_timestamp if latest_parent_timestamp > ts else ts
                logger.debug(f"After processing parent {parent}: any_paused_failed = {any_paused_failed}, any_stale_unav = {any_stale_unav}, latest_ts = {latest_parent_timestamp}")

            if any_paused_failed:
                asset.status = AssetStatus.UpstreamPaused
                logger.info(f"Asset {asset} status: {fmt(asset.status)} (parent paused or failed)")
                continue

            timestamp = asset._timestamp()
            if timestamp == AssetStatus.Unavailable:
                asset.status = AssetStatus.Unavailable
                logger.info(f"Asset {asset} status: {fmt(asset.status)} (timestamp unavailable)")
                continue

            if any_stale_unav:
                asset.status = AssetStatus.Stale
                logger.info(f"Asset {asset} status: {fmt(asset.status)} (parent stale/unavailable)")
                continue

            #check for unavailable; if so, since any_stale_unav = False, this is a root node with no parent, so move on
            if (latest_parent_timestamp != AssetStatus.Unavailable) and (timestamp < latest_parent_timestamp):
                asset.status = AssetStatus.Stale
                logger.info(f"Asset {asset} status: {fmt(asset.status)} (timestamp older than parent)")
                continue

            asset.status = AssetStatus.Current
            logger.info(f"Asset {asset} status: {fmt(asset.status)}")

        self._cleanup_resources(required_resources)

        return

    def _initialize_resources(self, resources):
        logger.info(f"Initializing {len(resources)} build resources")
        for resource in resources:
            try:
                logger.debug(f"Initializing resource {resource}")
                resource.init()
            except Exception as e:
                logger.exception(f"Resource {resource} initialization failed; cleaning up and aborting build")
                self._cleanup_resources(resources)
                return False
        return True

    def _cleanup_resources(self, resources):
        logger.info(f"Cleaning up {len(resources)} build resources")
        #cleanup resources
        for resource in resources:
            try:
                logger.debug(f"Cleaning up resource {resource}")
                resource.close()
            except Exception as e:
                logger.exception(f"Resource {resource} cleanup failed; attempting to clean up other resources")

    def build_upstream(self, asset_or_assets):
        logger.info(f"Building assets upstream of {asset_or_assets}")
        if isinstance(asset_or_assets, Asset):
            asset_or_assets = [asset_or_assets]
        nodes_to_build = []
        for asset in asset_or_assets:
            nodes_to_build.extend(nx.ancestors(self.graph, asset))
        sg = self.graph.subgraph(nodes_to_build)
        sorted_nodes = list(nx.topological_sort(sg))
        self._build(sorted_nodes)

    def build_downstream(self, asset_or_assets):
        logger.info(f"Building assets downstream of {asset_or_assets}")
        if isinstance(asset_or_assets, Asset):
            asset_or_assets = [asset_or_assets]
        nodes_to_build = []
        for asset in asset_or_assets:
            nodes_to_build.extend(nx.descendants(self.graph, asset))
        sg = self.graph.subgraph(nodes_to_build)
        sorted_nodes = list(nx.topological_sort(sg))
        self._build(sorted_nodes)

    def build(self):
        logger.info(f"Building all assets")
        sorted_nodes = list(nx.topological_sort(self.graph))
        self._build(sorted_nodes)

    def _build(self, sorted_nodes):
        # Builds only happen for Unavailable and Stale assets whose parents are all Current
        #if self._stale_topological_sort:
        #    logger.info(f"Asset graph structure changed; recomputing the topological sort")
        #    self._cached_topological_sort = list(nx.topological_sort(self.graph))
        #    self._stale_topological_sort = False

        # collect and initialize required resources
        required_resources = set()
        for asset in sorted_nodes:
            required_resources.update(asset.resources)
        if not self._initialize_resources(required_resources):
            return

        # loop over assets and build
        for asset in sorted_nodes:
            parent_status_cts = Counter([p.status for p in self.graph.predecessors(asset)])
            logger.debug(f"Checking asset {asset} with status {asset.status} and parent statuses {(' '.join([str(item[0]) +': ' + str(item[1]) + ',' for item in list(parent_status_cts.items())]))[:-1]}")
            if (asset.status == AssetStatus.Unavailable or asset.status == AssetStatus.Stale) and all(p.status == AssetStatus.Current for p in self.graph.predecessors(asset)):
                logger.info(f"Asset {asset} is {fmt(asset.status)} and all parents are current; rebuilding")
                # set building status
                asset.status = AssetStatus.Building
                asset._last_build_timestamp = plm.now()

                # run the build
                try:
                    logger.debug(f"Building {asset}")
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

        self._cleanup_resources(required_resources)

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
                summary += f"        {fmt(sg)}: {status_groups[sg]} subgroups/singletons\n"
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


    # TODO visualization (this code works but has old viznodes in it
    # TODO recode this with new _collect_groups function
    #def visualize(self):
    #    # in order to visualize large graphs
    #    # collapse all subgroups within a group with all constant state
    #    logger.info("Visualizing asset graph")
    #    groupings = {}
    #    logger.info("Assembling viznodes")
    #    for asset in self.graph:
    #        # if the asset is part of a group,subgroup
    #        if (asset.group is not None) and (asset.subgroup is not None):
    #            # collect assets in each group/subgroup
    #            if asset.group not in groupings:
    #                groupings[asset.group] = {}
    #            if asset.subgroup not in groupings[asset.group]:
    #                groupings[asset.group][asset.subgroup] = []
    #            groupings[asset.group][asset.subgroup].append(asset)
    #        else:
    #            #ungrouped assets get their own viznodes
    #            asset.viznode = ("single",asset.hash,asset.status)
    #    # for grouped assets, merge into one viznode if status is the same across the subgroup
    #    for group in groupings:
    #        for subgroup in groupings[group]:
    #            if len({asset.status for asset in groupings[group][subgroup]}) == 1:
    #                for asset in groupings[group][subgroup]:
    #                    asset.viznode = ("group",group,asset.status)
    #            else:
    #                for asset in groupings[group][subgroup]:
    #                    asset.viznode = ("single",asset.hash,asset.status)
    #    logger.info("Building vizgraph")
    #    # create the vizgraph
    #    vizgraph = nx.DiGraph()
    #    # add all viznodes
    #    for asset in self.graph:
    #        vizgraph.add_edges_from([(p.viznode, asset.viznode) for p in self.graph.predecessors(asset)])
    #    # remove any self-edges caused by viznode
    #    for node in vizgraph:
    #        try:
    #            vizgraph.remove_edge(node, node)
    #        except:
    #            pass
    #    logger.info(f"Quotient graph has {vizgraph.number_of_nodes()} nodes and {vizgraph.size()} edges.")
    #    logger.info("Computing quotient node positions")
    #    pos = nx.planar_layout(vizgraph)
    #    pos = nx.spring_layout(vizgraph, pos=pos) #initialize spring with planar
    #    logger.info("Drawing the graph")
    #    node_colors = [_NODE_COLORS[node[2]] for node in vizgraph]
    #    node_sizes = [600 if node[0] == "group" else 100 for node in vizgraph]
    #    nx.draw(vizgraph, pos=pos, node_color=node_colors, node_size=node_sizes)
    #    plt.show()







