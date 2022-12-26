import networkx as nx
from loguru import logger
from .asset import Asset, AssetStatus
#import matplotlib.pyplot as plt
from collections import Counter, defaultdict
from collections.abc import Iterable
import pendulum as plm
from notifiers.logging import NotificationHandler
from kombu import Connection, Exchange, Queue
from .schedule import Schedule
import asyncio
from enum import Enum
from queue import Queue as ThreadSafeQueue, Empty
from threading import Thread
from coolname import generate_slug

class ShutdownSignal(Exception):
    pass

class BuildResult(Enum):
    Rebuilt = 0
    Skipped = 1
    Failed = 2

__RABBIT_URL__ = "amqp://guest:guest@localhost//"
__MESSAGE_TTL__ = 1


_NODE_COLORS = {AssetStatus.Current : "tab:green",
                AssetStatus.Stale : "khaki",
                AssetStatus.Building : "tab:blue",
                AssetStatus.Paused : "slategray",
                AssetStatus.UpstreamStopped : "lightslategray",
                AssetStatus.Unavailable : "gray",
                AssetStatus.Failed : "tab:red"
            }

# TODO: use HEX to match above colors <fg #00005f>, <fg #EE1>
_LOGURU_COLORS = {AssetStatus.Current : "32",
                AssetStatus.Stale : "33",
                AssetStatus.Building : "34",
                AssetStatus.Paused : "35",
                AssetStatus.UpstreamStopped : "35",
                AssetStatus.Unavailable : "37",
                AssetStatus.Failed : "91"
            }

# TODO: use better formatting than ANSI...
def fmt(status):
    #return f"<{_LOGURU_COLORS[status]}>{status}</>"
    return f"\033[{_LOGURU_COLORS[status]}m{status}\033[00m\u001b[1m"

async def wait_for_dependencies(task, parents):
    for parent in parents:
        await parent
    result = await task
    return result

class AssetGraph:

    def __init__(self, key, notifiers = None):
        self.graph = nx.DiGraph()
        self.key = key
        self.schedules = {}
        if notifiers is not None:
            for service in notifiers:
                handler = NotificationHandler(service, defaults=notifiers[service]["params"])
                logger.add(handler, level=notifiers[service]["level"])
        self.message_queue = ThreadSafeQueue()
        self._stop_messaging_flag = False
        # map of action to whether it is read only w.r.t. the asset graph
        self._action_modifies_graph = {
            'refresh' : True,
            'build' : True,
            'cancel' : False,
            'summarize' : False,
            'ls' : False,
            'pause' : True,
            'unpause' : True,
            'schedule' : False,
            'unschedule' : False
        }

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

    def list_assets(self, display=True):
        s = ''
        if self.graph.number_of_nodes() == 0:
            s += "No assets to list."
        else:
            s += "Assets:\n"
            for asset in self.graph:
                s += f"- {asset.key}: {fmt(asset.status)}\n"
        if display:
            logger.info(s)
        return s

    def list_schedules(self, display=True):
        s = ''
        if len(self.schedules) == 0:
            s += "No schedules to list."
        else:
            s += "Schedules:\n"
            for sch in self.schedules:
                s += f"- {sch}: {self.schedules[sch]}"
        if display:
            logger.info(s)
        return s

    def schedule(self, schedule_key, action, cron_string, asset = None):
        logger.info(f"Adding new schedule with key {schedule_key}")
        if schedule_key in self.schedules:
            logger.error(f"Tried to add schedule key {schedule_key}, but it already exists. Skipping.")
            return
        # find asset corresponding to asset_key
        jobstr = f"{action} all" if asset is None else f"{action} {asset}"
        logger.info(f"Adding scheduled run '{schedule_key}' ({jobstr} at '{cron_string}') to graph {self.key}")
        self.schedules[schedule_key] = Schedule(action, cron_string, asset)

    def unschedule(self, schedule_key):
        logger.info(f"Removing scheduled run '{schedule_key}'")
        if schedule_key not in self.schedules:
            logger.warning(f"Schedule {schedule_key} not in schedules.")
        self.schedules.pop(schedule_key, None)

    def pause_schedule(self, schedule_key):
        logger.info(f"Pausing schedule '{schedule_key}'")
        self.schedules[schedule_key].pause()

    def pause_asset(self, asset):
        logger.info(f"Pausing asset {asset}")
        asset.status = AssetStatus.Paused
        self.refresh_downstream(asset)

    def unpause_schedule(self, schedule_key):
        logger.info(f"Resuming schedule '{schedule_key}'")
        self.schedules[schedule_key].unpause()

    def unpause_asset(self, asset):
        logger.info(f"Unpausing asset {asset}")
        asset.status = AssetStatus.Stale
        self.refresh_downstream(asset)

    def _on_message(self, body, message):
        logger.info(f"Message loop: received {body['type']} message")
        logger.debug(f"Message body: {body}")
        message.ack()
        self.message_queue.put(body)
        if body["type"] == "shutdown":
            self._stop_messaging_flag = True

    def _message_wait(self):
        exchange = Exchange('fwirl', 'direct', durable = False)
        queue = Queue(self.key, exchange=exchange, routing_key = self.key, message_ttl = 1., auto_delete=True)
        with Connection(__RABBIT_URL__) as conn:
            with conn.Consumer(queue, callbacks=[self._on_message]):
                while True:
                    conn.drain_events()
                    if self._stop_messaging_flag:
                        self._stop_messaging_flag = False
                        break
 
    def _get_message(self, timeout):
        try:
            msg = self.message_queue.get(timeout=timeout)
        except Empty:
            return None
        return msg

    def _send_message(self, schedule_key, msg):
        exch = Exchange('fwirl', 'direct', durable=False)
        queue = Queue(self.key+"-"+schedule_key, exchange=exch, routing_key=self.key+"-"+schedule_key, message_ttl = 1., auto_delete=True)
        with Connection(__RABBIT_URL__) as conn:
            producer = conn.Producer()
            producer.publish(msg, exchange=exch, routing_key = self.key+"-"+schedule_key, declare=[queue])

    def _process_message(self, msg):
        if msg["type"] == "summarize":
            self.summarize()

        if msg["type"] == "ls":
            if msg["assets"]:
                self.list_assets()
            if msg["schedules"]:
                self.list_schedules()

        if msg["type"] == "build":
            asset = None
            for _asset in self.graph:
                if _asset.key == msg["asset_key"]:
                    asset = _asset
            if asset is not None:
                self.build_upstream(asset)
            else:
                self.build()

        if msg["type"] == "refresh":
            asset = None
            for _asset in self.graph:
                if _asset.key == msg["asset_key"]:
                    asset = _asset
            if asset is not None:
                self.refresh_downstream(asset)
            else:
                self.refresh()

        if msg["type"] == "pause":
            asset = None
            for _asset in self.graph:
                if _asset.key == msg["key"]:
                    asset = _asset
            if asset is not None:
                self.pause_asset(asset)
            if msg["key"] in self.schedules:
                self.pause_schedule(msg["key"])

        if msg["type"] == "unpause":
            asset = None
            for _asset in self.graph:
                if _asset.key == msg["key"]:
                    asset = _asset
            if asset is not None:
                self.unpause_asset(asset)
            if msg["key"] in self.schedules:
                self.unpause_schedule(msg["key"])

        if msg["type"] == "schedule":
            asset = None
            for _asset in self.graph:
                if _asset.key == msg["asset_key"]:
                    asset = _asset
            self.schedule(msg["schedule_key"], msg["action"], msg["cron_str"], asset)

        if msg["type"] == "unschedule":
            self.unschedule(msg["schedule_key"])

    def run(self):
        # run the message handling loop
        # need a separate thread for this since conn.drain_events() blocks, and kombu isn't compatible with asyncio yet
        logger.info(f"Starting fwirl messaging loop")
        th = Thread(name = "fwirl_messaging_loop", target=self._message_wait, args=None, daemon=True)
        th.start()

        logger.info(f"Starting fwirl job event loop")
        # run the task executor/scheduler async
        try:
            asyncio.run(self._run())
        except KeyboardInterrupt:
            logger.info(f"Caught keyboard interrupt; stopping main loop and messaging loop of asset graph {self.key}")
            exchange = Exchange('fwirl', 'direct', durable = False)
            queue = Queue(self.key, exchange=exchange, routing_key = self.key, message_ttl = 1., auto_delete=True)
            with Connection(__RABBIT_URL__) as conn:
                producer = conn.Producer()
                producer.publish({"type": "shutdown"}, exchange=exchange, routing_key = self.key, declare=[queue])
        except ShutdownSignal:
            # this only happens if the messaging loop got the "shutdown" message, so it is already shutting itself down, no need to publish shutdown msg
            logger.info(f"Caught shutdown signal; stopping main loop of asset graph {self.key}")
        th.join()

    async def _run(self):
        job_queue = []
        job_running = []
        message_task = None
        while True:
            # remove paused schedules from job queue   
            job_queue = filter(lambda job: not self.schedules[job[0]].is_paused(), job_queue)
            for sk in self.schedules:
                if self.schedules[sk].next() == Schedule.IMMEDIATE:
                    # if the immediate job is already in the queue, just skip; otherwise add it
                    if sk not in [job[0] for job in job_queue]:
                        job_queue.put((sk, Schedule.IMMEDIATE))
                else:
                    # add all occurrences of each scheduled job onto the queue between the most recent one on the queue and the next future one
                    # get the latest current scheduled time for this schedule, or now if none scheduled
                    times_for_sk = [job[1] for job in job_queue if job[0] == sk]
                    if len(times_for_sk) > 0:
                        latest = max(times_for_sk)
                    else:
                        latest = plm.now() 
                    # loop over runs starting from most recent one on the queue until the next one after the current time, adding all 
                    while (not self.schedules[sk].is_paused()) and (latest <= plm.now()) and (self.schedules[sk].next(dt = latest) is not None):
                        new_time = latest + plm.duration(seconds = self.schedules[sk].next(dt = latest))
                        job_queue.put((sk, new_time))
                        latest = new_time

            # sort upcoming jobs by scheduled time
            job_queue.sort(key = lambda job : job[1])

            # loop through scheduled jobs from earliest
            # run each one until one would be incompatible with currently running
            # don't run multiple copies of the same schedule (otherwise if they're all read-only, we'd infinite loop)
            # if any jobs are running, wait indefinitely on message queue
            # if no jobs are running, wait until next job
            running_keys = [j[0] for j in job_running]
            running_jobs_modify_graph = any([j[1].modifies_graph for j in job_running])
            while len(job_queue) > 0 and job_queue[0][1] < plm.now():
                if (running_jobs_modify_graph and self.schedules[job_queue[0][0]].modifies_graph) or (job_queue[0][0] in running_keys):
                    break
                # pop the next job
                next_sk, next_time = job_queue.pop(0)
                
                if next_time == Schedule.IMMEDIATE:
                    logger.info(f"Running event {next_sk} ({self.schedules[next_sk]}) immediately")
                else:
                    # check if it's late (30 second window)
                    logger.info(f"Running event {next_sk} ({self.schedules[next_sk]}) scheduled for {next_time} now")
                    if next_time + plm.duration(seconds=30) < plm.now():
                        logger.warning(f"Scheduled event {next_sk} is late; supposed to run at {next_time}, time now {plm.now()}")
                        logger.warning(f"Job queue currently has length {len(job_queue)}")
                
                # run the job
                task = asyncio.create_task(self.schedules[next_sk].generate_coroutine())
                job_running.append((next_sk, self.schedules[next_sk], generate_slug(2), task)) 

                # if it was an immediate job, remove the schedule
                if next_time == Schedule.IMMEDIATE:
                    self.schedules.pop(next_sk)
 
            # if no message task is running, start one
            if message_task is None:
                # if jobs are running, just wait indefinitely for next message
                # if no jobs are running, wait for next job
                timeout = None
                if len(job_running) == 0 and len(job_queue) > 0: 
                    timeout = (job_queue[0][1] - plm.now()).seconds 
                    #note: job_queue[0][1] should never be IMMEDIATE here, but if it somehow is, assert
                    assert job_queue[0][1] != Schedule.IMMEDIATE
                message_task = asyncio.create_task(asyncio.to_thread(self._get_message, timeout))

            # wait for next proc
            await asyncio.wait([j[3] for j in job_running] + [message_task],  return_when=asyncio.FIRST_COMPLETED)

            # if the message task is done, process it
            if message_task.done():
                msg = await message_task
                self._process_msg(msg)
                message_task = None
            
            # loop over jobs that ran and clear out completed tasks
            for job in job_running:
                if job[3].done():
                    res = await job[3]
            job_running = filter(lambda j : not j[3].done(), job_running)


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

    def refresh(self):
        logger.info(f"Refreshing all assets")
        sorted_nodes = list(nx.topological_sort(self.graph))
        asyncio.run(self._refresh(sorted_nodes))

    def refresh_downstream(self, asset_or_assets):
        logger.info(f"Refreshing assets downstream of (and including) {asset_or_assets}")
        if isinstance(asset_or_assets, Asset):
            asset_or_assets = [asset_or_assets]
        nodes_to_refresh = []
        for asset in asset_or_assets:
            nodes_to_refresh.append(asset)
            nodes_to_refresh.extend(nx.descendants(self.graph, asset))
        sg = self.graph.subgraph(nodes_to_refresh)
        sorted_nodes = list(nx.topological_sort(sg))
        asyncio.run(self._refresh(sorted_nodes))

    async def _refresh(self, sorted_nodes):
        required_resources = set()
        for asset in sorted_nodes:
            required_resources.update(asset.resources)
        if not self._initialize_resources(required_resources):
            return

        task_map = {}
        for asset in sorted_nodes:
            coroutine = self._refresh_asset(asset)
            task = asyncio.create_task(wait_for_dependencies(coroutine, [task_map[a] for a in self.graph.predecessors(asset)]))
            task_map[asset] = task

        for asset in sorted_nodes:
            await task_map[asset]

        self._cleanup_resources(required_resources)
        return

    def build(self):
        logger.info(f"Building all assets")
        sorted_nodes = list(nx.topological_sort(self.graph))
        needs_rerun = True
        while needs_rerun:
            needs_rerun = asyncio.run(self._build(sorted_nodes))

    def build_upstream(self, asset_or_assets):
        logger.info(f"Building assets upstream of (and including) {asset_or_assets}")
        if isinstance(asset_or_assets, Asset):
            asset_or_assets = [asset_or_assets]
        nodes_to_build = []
        for asset in asset_or_assets:
            nodes_to_build.append(asset)
            nodes_to_build.extend(nx.ancestors(self.graph, asset))
        sg = self.graph.subgraph(nodes_to_build)
        sorted_nodes = list(nx.topological_sort(sg))
        needs_rerun = True
        while needs_rerun:
            needs_rerun = asyncio.run(self._build(sorted_nodes))

    async def _build(self, sorted_nodes):
        required_resources = set()
        for asset in sorted_nodes:
            required_resources.update(asset.resources)
        if not self._initialize_resources(required_resources):
            return

        # loop over assets, refresh and build
        task_map = {}
        for asset in sorted_nodes:
            coroutine = self._refresh_asset(asset)
            _task = asyncio.create_task(wait_for_dependencies(coroutine, [task_map[a] for a in self.graph.predecessors(asset)]))
            coroutine = self._build_asset(asset)
            task = asyncio.create_task(wait_for_dependencies(coroutine, [_task]))
            task_map[asset] = task

        all_successful = True
        workers_to_notify = {}
        for asset in sorted_nodes:
            build_result = await task_map[asset]
            all_successful = all_successful and (build_result != BuildResult.Failed)
            if build_result == BuildResult.Rebuilt:
                for worker in self.workers:
                    if asset in worker.watched_assets:
                        workers_to_notify.add(worker)

        self._cleanup_resources(required_resources)

        summary = self.summarize(display=False)
        if not all_successful:
            logger.info(f"Build failure occurred")
            logger.error(summary, colorize=True)
        else:
            logger.info(f"Build successful")
            logger.info(summary, colorize=True)

        if len(workers_to_notify) > 0:
            logger.info(f"Build triggered graph workers. Restructuring...")
            any_changed = False
            for worker in workers_to_notify:
                logger.info(f"Running graph worker {worker}")
                changed = worker.restructure(self.graph)
                any_changed = any_changed or changed
            if any_changed:
                logger.info(f"Graph structure changed. Triggering rebuild...")
            else:
                logger.info(f"Graph structure unchanged. No rebuild necessary")
            return any_changed
        else:
            logger.info(f"No graph workers triggered during build.")
            return False

    async def _refresh_asset(self, asset):
        # Status refresh propagates using the following cascade of rules:
        # 0. If I'm Failed and self.allow_retry = False, I'm Failed
        # 0. If I'm Paused, I'm Paused
        # 1. If any of my parents is Paused or UpstreamStopped or Failed, I'm UpstreamStopped.
        # 2. If I don't exist, I'm Unavailable.
        # 3. If any of my parents is Stale or Unavailable, I'm Stale
        # 4. All of my parents are Current. So check timestamps. If my timestamp is earlier than any of them, I'm Stale.
        # 5. I'm Current
        logger.debug(f"Updating status for asset {asset}")

        if (not asset.allow_retry) and (asset.status == AssetStatus.Failed):
            logger.info(f"Asset {asset} status: {fmt(asset.status)} (previous failure and asset does not allow retries)")
            return

        if asset.status == AssetStatus.Paused:
            logger.info(f"Asset {asset} status: {fmt(asset.status)} (paused asset)")
            return

        any_paused_failed = False
        any_stale_unav = False
        latest_parent_timestamp = AssetStatus.Unavailable
        for parent in self.graph.predecessors(asset):
            logger.debug(f"Checking parent {parent} with status {parent.status} and timestamp {await parent.timestamp()}")
            if parent.status == AssetStatus.Paused or parent.status == AssetStatus.UpstreamStopped or parent.status == AssetStatus.Failed:
                any_paused_failed = True
            if parent.status == AssetStatus.Stale or parent.status == AssetStatus.Unavailable:
                any_stale_unav = True
            # don't try to compute timestamps if any parent is paused, since they may not exist (and have no cached ts)
            if parent.status != AssetStatus.Unavailable and (not any_paused_failed):
                ts = await parent.timestamp()
                if latest_parent_timestamp == AssetStatus.Unavailable:
                    latest_parent_timestamp = ts
                else:
                    latest_parent_timestamp = latest_parent_timestamp if latest_parent_timestamp > ts else ts
            logger.debug(f"After processing parent {parent}: any_paused_failed = {any_paused_failed}, any_stale_unav = {any_stale_unav}, latest_ts = {latest_parent_timestamp}")

        if any_paused_failed:
            asset.status = AssetStatus.UpstreamStopped
            logger.info(f"Asset {asset} status: {fmt(asset.status)} (parent paused or failed)")
            return

        timestamp = await asset.timestamp()
        if timestamp == AssetStatus.Unavailable:
            asset.status = AssetStatus.Unavailable
            logger.info(f"Asset {asset} status: {fmt(asset.status)} (timestamp unavailable)")
            return

        if any_stale_unav:
            asset.status = AssetStatus.Stale
            logger.info(f"Asset {asset} status: {fmt(asset.status)} (parent stale/unavailable)")
            return

        #check for unavailable; if so, since any_stale_unav = False, this is a root node with no parent, so move on
        if (latest_parent_timestamp != AssetStatus.Unavailable) and (timestamp < latest_parent_timestamp):
            asset.status = AssetStatus.Stale
            logger.info(f"Asset {asset} status: {fmt(asset.status)} (timestamp older than parent)")
            return

        asset.status = AssetStatus.Current
        logger.info(f"Asset {asset} status: {fmt(asset.status)}")

    async def _build_asset(self, asset):
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
                await asset.build()
            except Exception as e:
                asset.status = AssetStatus.Failed
                asset.message = "Build failed: exception during build"
                logger.exception(f"Asset {asset} build failed (status: {asset.status}): error in-build")
                return BuildResult.Failed

            # refresh the timestamp cache
            ts = await asset.timestamp()
            logger.debug(f"New asset {asset} timestamp: {ts}")

            # get latest parent timestamp
            parents = list(self.graph.predecessors(asset))
            parent_timestamps = [(await p.timestamp()) for p in parents]
            if len(parents) == 0:
                logger.debug(f"Latest parent asset timestamp: (no parents)")
            else:
                latest_parent_timestamp = max(parent_timestamps)
                logger.debug(f"Latest parent asset timestamp: {latest_parent_timestamp}")
                if ts < latest_parent_timestamp:
                    raise RuntimeError("Rebuilt asset has timestamp earlier than latest parent!")

            # make sure asset is now available and has a new timestamp
            if ts is AssetStatus.Unavailable:
                asset.status = AssetStatus.Failed
                asset.message = "Build failed: asset unavailable"
                logger.error(f"Asset {asset} build failed (status: {asset.status}): asset unavailable after build")
                return BuildResult.Failed

            # ensure that it now has a newer timestamp than parents
            if not all(pt < ts for pt in parent_timestamps):
                asset.status = AssetStatus.Failed
                asset.message = "Build failed: asset timestamp older than parents"
                logger.error(f"Asset {asset} build failed (status: {asset.status}): asset timestamp older than parents")
                return BuildResult.Failed

            # update to current status
            asset.status = AssetStatus.Current
            asset.message = "Build complete"
            logger.info(f"Asset {asset} build completed successfully (status: {fmt(asset.status)})")
        else:
            logger.debug(f"Asset {asset} not ready to rebuild. Skipping.")
            return BuildResult.Skipped

        return BuildResult.Rebuilt

    def _collect_groups(self):
        logger.debug("Collecting node (sub)groups")
        groupings = defaultdict(list)
        # ensure __singletons__ group exists
        groupings['__singletons__'] = []
        for asset in self.graph:
            if asset.group == '__singletons__' or asset.subgroup == '__singletons__':
                raise ValueError('(Sub)group name used reserved keyword __singletons__')
            if asset.group is not None:
                if asset.group not in groupings:
                    groupings[asset.group] = defaultdict(list)
                    # ensure __singletons__ subgroup  exists
                    groupings[asset.group]['__singletons__'] = []
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

    def summarize(self, display=True):
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
        if display:
            logger.info(summary, colorize=True)
        return summary


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







