from kombu import Connection, Exchange, Queue
from loguru import logger

__RABBIT_URL__ = "amqp://guest:guest@localhost//"
__EXCH_NAME__ = "fwirl"

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

