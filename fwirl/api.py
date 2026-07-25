import pendulum as plm
from coolname import generate_slug
from .message import get_msg, publish_msg, listen, list_running_graphs, __RABBIT_URL__
from queue import Queue


def summarize(graph_key, rabbit_url=__RABBIT_URL__):
    """Print a summary of the running asset graph to stdout.

    Sends a ``summarize`` message to the graph server identified by
    *graph_key* and waits for its response.

    Args:
        graph_key: The string key of the running :class:`~fwirl.AssetGraph`.
        rabbit_url: RabbitMQ connection URL.  Defaults to
            ``******localhost//``.
    """
    resp_name = 'summarize-'+generate_slug(2)
    publish_msg(graph_key, {"type": "summarize", "resp_queue": resp_name}, rabbit_url) 
    queue = Queue()
    try:
        get_msg(resp_name, queue, rabbit_url)
    except KeyboardInterrupt:
        print("Caught keyboard interrupt; quitting")
        quit()
    print(queue.get()['response'])


def list_graphs():
    """List the graph keys of currently running asset graph servers.

    Returns:
        list[str]: Sorted graph keys discovered on the local machine.
    """
    graphs = list_running_graphs()
    if len(graphs) == 0:
        print("No running graphs found.")
    else:
        for graph_key in graphs:
            print(graph_key)
    return graphs


def ls(graph_key, assets=False, schedules=False, jobs=False, rabbit_url=__RABBIT_URL__):
    """List the contents of a running asset graph.

    Sends an ``ls`` message to the graph server and prints the response.
    Pass one or more flags to control which information is included.

    Args:
        graph_key: The string key of the running :class:`~fwirl.AssetGraph`.
        assets: When ``True``, include the list of assets and their statuses.
        schedules: When ``True``, include the list of schedules.
        jobs: When ``True``, include the job queue.
        rabbit_url: RabbitMQ connection URL.
    """
    resp_name = 'ls-'+generate_slug(2)
    publish_msg(graph_key, {"type": "ls", "resp_queue": resp_name, "assets": assets, "schedules": schedules, "jobs": jobs}, rabbit_url) 
    queue = Queue()
    try:
        get_msg(resp_name, queue, rabbit_url)
    except KeyboardInterrupt:
        print("Caught keyboard interrupt; quitting")
        quit()
    print(queue.get()['response'])


def shutdown(graph_key, rabbit_url=__RABBIT_URL__):
    """Send a shutdown signal to a running asset graph server.

    Args:
        graph_key: The string key of the running :class:`~fwirl.AssetGraph`.
        rabbit_url: RabbitMQ connection URL.
    """
    publish_msg(graph_key, {"type": "shutdown"}, rabbit_url) 


def refresh(graph_key, asset_key=None, rabbit_url=__RABBIT_URL__):
    """Trigger a status refresh on a running asset graph.

    A refresh checks the timestamps of every asset (or just the specified
    asset and its downstream dependents) without rebuilding any of them.

    Args:
        graph_key: The string key of the running :class:`~fwirl.AssetGraph`.
        asset_key: Optional string key of a specific asset to refresh.
            When ``None`` (the default) all assets are refreshed.
        rabbit_url: RabbitMQ connection URL.
    """
    publish_msg(graph_key, {"type": "refresh", "asset_key": asset_key}, rabbit_url)


def build(graph_key, asset_key=None, rabbit_url=__RABBIT_URL__):
    """Trigger a build on a running asset graph.

    Rebuilds all stale or unavailable assets (or the specified asset and
    its upstream dependencies).

    Args:
        graph_key: The string key of the running :class:`~fwirl.AssetGraph`.
        asset_key: Optional string key of a specific asset to build.
            When ``None`` (the default) all assets are built.
        rabbit_url: RabbitMQ connection URL.
    """
    publish_msg(graph_key, {"type": "build", "asset_key": asset_key}, rabbit_url)


def pause(graph_key, key=None, rabbit_url=__RABBIT_URL__):
    """Pause an asset or schedule in a running asset graph.

    A paused asset will not be rebuilt until it is unpaused.  A paused
    schedule will not fire until it is unpaused.

    Args:
        graph_key: The string key of the running :class:`~fwirl.AssetGraph`.
        key: String key of the asset or schedule to pause.
        rabbit_url: RabbitMQ connection URL.
    """
    publish_msg(graph_key, {"type": "pause", "key": key}, rabbit_url)


def unpause(graph_key, key=None, rabbit_url=__RABBIT_URL__):
    """Resume a previously paused asset or schedule.

    Args:
        graph_key: The string key of the running :class:`~fwirl.AssetGraph`.
        key: String key of the asset or schedule to unpause.
        rabbit_url: RabbitMQ connection URL.
    """
    publish_msg(graph_key, {"type": "unpause", "key": key}, rabbit_url)


def schedule(graph_key, schedule_key, action, cron_string, asset_key=None, rabbit_url=__RABBIT_URL__):
    """Add a new recurring schedule to a running asset graph.

    Args:
        graph_key: The string key of the running :class:`~fwirl.AssetGraph`.
        schedule_key: A unique string identifier for the new schedule.
        action: The action to perform; either ``"refresh"`` or ``"build"``.
        cron_string: A standard cron expression controlling when the
            schedule fires (e.g. ``"0 * * * *"`` for every hour).
        asset_key: Optional string key of the asset targeted by the action.
            When ``None`` the action applies to all assets.
        rabbit_url: RabbitMQ connection URL.
    """
    publish_msg(graph_key, {"type": "schedule", "schedule_key": schedule_key, "action": action, "cron_string": cron_string, "asset_key": asset_key}, rabbit_url)


def unschedule(graph_key, schedule_key, rabbit_url=__RABBIT_URL__):
    """Remove an existing schedule from a running asset graph.

    Args:
        graph_key: The string key of the running :class:`~fwirl.AssetGraph`.
        schedule_key: The string key of the schedule to remove.
        rabbit_url: RabbitMQ connection URL.
    """
    publish_msg(graph_key, {"type": "unschedule", "schedule_key": schedule_key}, rabbit_url)
