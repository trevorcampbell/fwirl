from kombu import Connection, Queue, Exchange
import pendulum as plm

__RABBIT_URL__ = "amqp://guest:guest@localhost//"

def _get_exch_queue(graph_key):
    exch = Exchange('sentry', 'direct', durable=False)
    queue = Queue(graph_key, exchange=exch, routing_key=graph_key)
    return exch, queue

def summarize(graph_key, rabbit_url = __RABBIT_URL__):
    exch, queue = _get_exch_queue(graph_key)
    with Connection(rabbit_url) as conn:
        producer = conn.Producer()
        producer.publish("summarize", exchange=exch, routing_key = graph_key, declare=[queue])
    # TODO consume a response queue for output


# TODO:
# - pause asset (certain/all assets)
# - clear failure (certain/all assets)
# - update status now (certain/all assets)
# - run build now (certain/all assets)
# - print summary of status
# - update schedule for an ongoing build/update task
