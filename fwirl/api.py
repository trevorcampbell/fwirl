from kombu import Connection, Queue, Exchange
import pendulum as plm

__RABBIT_URL__ = "amqp://guest:guest@localhost//"

def _get_exch_queue(graph_key):
    exch = Exchange('fwirl', 'direct', durable=False)
    queue = Queue(graph_key, exchange=exch, routing_key=graph_key, message_ttl = 1., auto_delete=True)
    return exch, queue

def summarize(graph_key, rabbit_url = __RABBIT_URL__):
    exch, queue = _get_exch_queue(graph_key)
    with Connection(rabbit_url) as conn:
        producer = conn.Producer()
        producer.publish({"type": "summarize"}, exchange=exch, routing_key = graph_key, declare=[queue])
    # TODO consume a response queue for output?

def ls(graph_key, assets = False, schedules = False, rabbit_url = __RABBIT_URL__):
    exch, queue = _get_exch_queue(graph_key)
    with Connection(rabbit_url) as conn:
        producer = conn.Producer()
        producer.publish({"type": "ls", "assets" : assets, "schedules" : schedules}, exchange=exch, routing_key = graph_key, declare=[queue])

def refresh(graph_key, asset_key = None, rabbit_url = __RABBIT_URL__):
    exch, queue = _get_exch_queue(graph_key)
    with Connection(rabbit_url) as conn:
        producer = conn.Producer()
        producer.publish({"type": "refresh", "asset_key" : asset_key}, exchange=exch, routing_key = graph_key, declare=[queue])

def build(graph_key, asset_key = None, rabbit_url = __RABBIT_URL__):
    exch, queue = _get_exch_queue(graph_key)
    with Connection(rabbit_url) as conn:
        producer = conn.Producer()
        producer.publish({"type": "build", "asset_key" : asset_key}, exchange=exch, routing_key = graph_key, declare=[queue])

def pause(graph_key, key=None, rabbit_url = __RABBIT_URL__):
    exch, queue = _get_exch_queue(graph_key)
    with Connection(rabbit_url) as conn:
        producer = conn.Producer()
        producer.publish({"type": "pause", "key" : key}, exchange=exch, routing_key = graph_key, declare=[queue])

def unpause(graph_key, key=None, rabbit_url = __RABBIT_URL__):
    exch, queue = _get_exch_queue(graph_key)
    with Connection(rabbit_url) as conn:
        producer = conn.Producer()
        producer.publish({"type": "unpause", "key" : key}, exchange=exch, routing_key = graph_key, declare=[queue])

def schedule(graph_key, schedule_key, action, cron_str, asset_key=None, rabbit_url = __RABBIT_URL__):
    exch, queue = _get_exch_queue(graph_key)
    with Connection(rabbit_url) as conn:
        producer = conn.Producer()
        producer.publish({"type": "schedule", "schedule_key" : schedule_key, "action" : action, "cron_str" : cron_str, "asset_key" : asset_key}, exchange=exch, routing_key = graph_key, declare=[queue])

def unschedule(graph_key, schedule_key, rabbit_url = __RABBIT_URL__):
    exch, queue = _get_exch_queue(graph_key)
    with Connection(rabbit_url) as conn:
        producer = conn.Producer()
        producer.publish({"type": "unschedule", "schedule_key" : schedule_key}, exchange=exch, routing_key = graph_key, declare=[queue])

