from kombu import Connection, Queue, Exchange
import pendulum as plm

__RABBIT_URL__ = "amqp://guest:guest@localhost//"

def _get_exch_queue(graph_key):
    exch = Exchange('sentry', 'direct', durable=False)
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

def refresh(graph_key, rabbit_url = __RABBIT_URL__):
    exch, queue = _get_exch_queue(graph_key)
    with Connection(rabbit_url) as conn:
        producer = conn.Producer()
        producer.publish({"type": "refresh"}, exchange=exch, routing_key = graph_key, declare=[queue])

def build(graph_key, rabbit_url = __RABBIT_URL__):
    exch, queue = _get_exch_queue(graph_key)
    with Connection(rabbit_url) as conn:
        producer = conn.Producer()
        producer.publish({"type": "build"}, exchange=exch, routing_key = graph_key, declare=[queue])

def pause(graph_key, asset=None, schedule=None, rabbit_url = __RABBIT_URL__):
    exch, queue = _get_exch_queue(graph_key)
    with Connection(rabbit_url) as conn:
        producer = conn.Producer()
        producer.publish({"type": "pause", "asset" : asset, "schedule" : schedule}, exchange=exch, routing_key = graph_key, declare=[queue])

def unpause(graph_key, asset=None, schedule=None, rabbit_url = __RABBIT_URL__):
    exch, queue = _get_exch_queue(graph_key)
    with Connection(rabbit_url) as conn:
        producer = conn.Producer()
        producer.publish({"type": "unpause", "asset" : asset, "schedule" : schedule}, exchange=exch, routing_key = graph_key, declare=[queue])

def schedule(graph_key, schedule_key, schedule, rabbit_url = __RABBIT_URL__):
    exch, queue = _get_exch_queue(graph_key)
    with Connection(rabbit_url) as conn:
        producer = conn.Producer()
        producer.publish({"type": "schedule", "key" : schedule_key, "schedule" : schedule}, exchange=exch, routing_key = graph_key, declare=[queue])

def unschedule(graph_key, schedule_key, rabbit_url = __RABBIT_URL__):
    exch, queue = _get_exch_queue(graph_key)
    with Connection(rabbit_url) as conn:
        producer = conn.Producer()
        producer.publish({"type": "unschedule", "key" : schedule_key}, exchange=exch, routing_key = graph_key, declare=[queue])

