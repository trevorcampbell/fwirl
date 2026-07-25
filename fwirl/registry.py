import os
import psutil
import tempfile
import glob
import json

__GRAPH_PID_PREFIX__ = "fwirlgraphpid_"

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

