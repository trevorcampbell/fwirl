import asyncio
import threading
import os, sys
import tempfile
import glob
import psutil
import pickle
import base64
from loguru import logger
from coolname import generate_slug
from aiohttp import web
from queue import Queue
from .message import get_msg,publish_msg,__RABBIT_URL__


__SERVERPORT__ = 8081

def getgraph(graph_key, rabbit_url = __RABBIT_URL__):
    resp_name = 'graph-'+generate_slug(2)
    publish_msg(graph_key, {"type": "graph", "resp_queue": resp_name}, rabbit_url) 
    queue = Queue()

    get_msg(resp_name, queue, rabbit_url)
    return queue.get()['response']


def aiohttp_server():
    def handle_request(request):
        graph_key = request.match_info['graph_key']
        encoded = getgraph(graph_key)
        decoded = base64.b64decode(encoded)
        svg = pickle.loads(decoded)
        
        logger.info(encoded)
        return web.Response(text=svg.decode('utf-8'),content_type='text/html')

    app = web.Application()
    app.add_routes([web.get('/graphs/{graph_key}', handle_request)])
    runner = web.AppRunner(app)
    return runner


def run_server(runner):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(runner.setup())
    site = web.TCPSite(runner, '127.0.0.1', __SERVERPORT__)
    loop.run_until_complete(site.start())
    loop.run_forever()

def start_webserver():
    fpid = os.fork()
    if fpid != 0:
        with tempfile.NamedTemporaryFile(mode='w', delete=False, prefix='fwirlserverpid_', dir=tempfile.gettempdir()) as temp_file:
            temp_file.write(str(fpid))
        sys.exit(0)

    logger.info(f"Starting webserver on port {__SERVERPORT__}...")
    t = threading.Thread(target=run_server, args=(aiohttp_server(),))
    t.start()
        
def stop_webserver():
    temp_dir = tempfile.gettempdir()
    pid_files = glob.glob(os.path.join(temp_dir, 'fwirlserverpid_*'))

    logger.info("Stopping webserver...")

    for pid_file in pid_files:
        try:
            with open(pid_file, 'r') as temp_file:
                pid = int(temp_file.read().strip())
        except:
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
        except:
            pass






