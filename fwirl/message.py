from kombu import Connection, Exchange, Queue

__RABBIT_URL__ = "amqp://guest:guest@localhost//"
__EXCH_NAME__ = "fwirl"

class FlagContainer:
    def __init__(self):
        flag=False

def _get_exch_queue(key):
    exch = Exchange(__EXCH_NAME__, 'direct', durable=False)
    queue = Queue(key, exchange=exch, routing_key=key, message_ttl = 1., auto_delete=True)
    return exch, queue

def _push_handler(queue, body, message):
    message.ack()
    queue.put(body)

def listen(key, handler_queue):
    exch, queue = _get_exch_queue(key)
    with Connection(__RABBIT_URL__) as conn:
        with conn.Consumer(queue, callbacks=[lambda  b, m : _push_handler(handler_queue, b, m)]):
            while True:
                conn.drain_events()
                if any([msg["type"] == "shutdown" for msg in handler_queue]):
                    break

def get(key, handler_queue):
    exch, queue = _get_exch_queue(key)
    with Connection(__RABBIT_URL__) as conn:
        with conn.Consumer(queue, callbacks=[lambda  b, m : _push_handler(handler_queue, b, m)]):
            conn.drain_events()
    

def publish(key, body):
    exch, queue = _get_exch_queue(key)
    with Connection(__RABBIT_URL__) as conn:
        producer = conn.Producer()
        producer.publish(body, exchange=exch, routing_key = key, declare=[queue])

