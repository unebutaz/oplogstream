from pika import BlockingConnection, ConnectionParameters, PlainCredentials, SelectConnection, TornadoConnection
from pprint import pprint
from bson import json_util, BSON
import json
import logging

DUMP_JSON = 1
DUMP_BSON = 2

logger = logging.getLogger('__main__')

class OpHandler(object):

    def handle(self, op):
        raise NotImplementedError("Please Implement this method")


class PrintOpHandler(OpHandler):

    def handle(self, op):
        pprint(op)


class QueueHandler(OpHandler):
    def __init__(self, host=None, port=None, vhost=None, username=None,
                 password=None, exchange='oplog', queue='', dump=DUMP_JSON):

        parameters = ConnectionParameters()

        if host is not None:
            parameters.host = host
        if port is not None:
            parameters.port = port
        if vhost is not None:
            parameters.virtual_host = vhost
        if username is not None and password is not None:
            parameters.credentials = PlainCredentials(username, password)

        logger.info('Connect to queue.')
        channel = BlockingConnection(parameters).channel()

        if queue is None:
            channel.exchange_declare(exchange, 'fanout', passive=True)
        else:
            channel.exchange_delete(exchange)
            channel.exchange_declare(exchange, 'direct')
            channel.queue_declare(queue, durable=True)
            channel.queue_bind(queue, exchange)

        self.exchange = exchange
        self.channel = channel
        self.queue = queue

        if dump == DUMP_JSON:
            self.dump = lambda op: json.dumps(op, default=json_util.default)
        elif dump == DUMP_BSON:
            self.dump = lambda op: BSON.encode(op)
        else:
            raise ValueError('Invalid `dump` parameter for QueueHandler.')

    def handle(self, op):
        try:
            self.channel.basic_publish(
                exchange=self.exchange, routing_key=self.queue, body=self.dump(op)
            )
        except BaseException as err:
            logging.error(err.message)