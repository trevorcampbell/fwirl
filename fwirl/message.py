import glob
import json
import os
import tempfile

import psutil
from kombu import Connection, Exchange, Queue
from loguru import logger

__RABBIT_URL__ = "amqp://guest:guest@localhost//"
__EXCH_NAME__ = "fwirl"
__GRAPH_PID_PREFIX__ = "fwirlgraphpid_"

class StopFlag:
    def __init__(self):
        self.flag=False

def _get_exch_queue(key):
    exch = Exchange(__EXCH_NAME__, 'direct', durable=False)
    queue = Queue(key, exchange=exch, routing_key=key, message_ttl = 1., auto_delete=True)
    return exch, queue

def _push_handler(queue, stop, body, message):
    logger.info(f"Messaging: received {body['type']} message")
    logger.debug(f"Message body: {body}")
    message.ack()
    queue.put(body)
    if body["type"] == "shutdown":
        stop.flag = True

def listen(key, handler_queue, url = __RABBIT_URL__):
    exch, queue = _get_exch_queue(key)
    stop = StopFlag()
    with Connection(__RABBIT_URL__) as conn:
        with conn.Consumer(queue, callbacks=[lambda  b, m : _push_handler(handler_queue, stop, b, m)]):
            while True:
                conn.drain_events()
                if stop.flag:
                    break

def get_msg(key, handler_queue, url = __RABBIT_URL__):
    stop = StopFlag()
    exch, queue = _get_exch_queue(key)
    with Connection(__RABBIT_URL__) as conn:
        with conn.Consumer(queue, callbacks=[lambda  b, m : _push_handler(handler_queue, stop, b, m)]):
            conn.drain_events()
    
def publish_msg(key, body, url = __RABBIT_URL__):
    exch, queue = _get_exch_queue(key)
    with Connection(__RABBIT_URL__) as conn:
        producer = conn.Producer()
        producer.publish(body, exchange=exch, routing_key = key, declare=[queue])


def register_graph_process(graph_key, pid=None):
    graphs = list_running_graphs()
    if graph_key in graphs:
        raise ValueError(f"Graph with name {graph_key} already exists") 
    
    pid = os.getpid() if pid is None else pid
    process = psutil.Process(pid)
    graph_info = {
        "graph_key": graph_key,
        "pid": pid,
        "create_time": process.create_time(),
    }
    with tempfile.NamedTemporaryFile(
        mode="w", delete=False, prefix=__GRAPH_PID_PREFIX__, dir=tempfile.gettempdir()
    ) as temp_file:
        json.dump(graph_info, temp_file)
        return temp_file.name


def unregister_graph_process(registration_file):
    if registration_file is None:
        return
    try:
        os.unlink(registration_file)
    except FileNotFoundError:
        pass


def list_running_graphs():
    graph_keys = set()
    pid_files = glob.glob(os.path.join(tempfile.gettempdir(), f"{__GRAPH_PID_PREFIX__}*"))
    for pid_file in pid_files:
        remove_file = False
        graph_key = None
        try:
            with open(pid_file, "r") as temp_file:
                graph_info = json.load(temp_file)
            pid = int(graph_info["pid"])
            create_time = float(graph_info["create_time"])
            graph_key = graph_info["graph_key"]
            process = psutil.Process(pid)
            if process.create_time() != create_time:
                remove_file = True
        except (FileNotFoundError, KeyError, TypeError, ValueError, json.JSONDecodeError, psutil.Error):
            remove_file = True
        if remove_file:
            unregister_graph_process(pid_file)
            continue
        if graph_key:
            graph_keys.add(graph_key)
    return sorted(graph_keys)
