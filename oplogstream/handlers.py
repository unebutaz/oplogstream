from pprint import pprint
from pika import BlockingConnection, ConnectionParameters

class OpHandler(object):

    def __init__(self, filtr=None):
        self.filter = filtr if filtr is not None else lambda x: x

    def handle(self, op):
        raise NotImplementedError("Please Implement this method")


class PrintOpHandler(OpHandler):
    def handle(self, op):
        if self.filter(op):
            pprint(op)


class QueueHandler(OpHandler):

    def __init__(self, exchange='oplog'):
        super(OpHandler, self).__init__()
        channel = BlockingConnection(ConnectionParameters('localhost')).channel()
        channel.exchange_declare(exchange, 'fanout')
        self.exchange = exchange
        self.channel = channel

    def handle(self, op):
        self.channel.basic_publish(exchange=self.exchange, routing_key='', body=op)

