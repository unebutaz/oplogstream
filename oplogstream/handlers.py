from pika import BlockingConnection, ConnectionParameters, PlainCredentials
from pprint import pprint
from bson import json_util, BSON
import threading
import Queue
import json
import logging

DUMP_JSON = 1
DUMP_BSON = 2

logger = logging.getLogger('__main__')

class OpHandler(object):

    def __init__(self):
        self.filters = []

    def add_filter(self, filter):
        if isinstance(filter, OpFilter):
            self.filters.append(filter)
        else:
            raise AttributeError("Not `OpFilter` instance")

    def handle(self, op):
        raise NotImplementedError("Please Implement this method")

class OpFilter(object):

    def __init__(self, dbs, colls, ops):
        self.dbs = dbs
        self.colls = colls
        self.ops = ops

    def is_valid(self, ns, op):
        if not ns:
            return False
        db, table = ns.split('.', 1)
        if len(self.dbs) > 0 and db not in self.dbs:
            return False
        if len(self.colls) > 0 and table not in self.colls:
            return False
        if len(self.ops) > 0 and op not in self.ops:
            return False
        return True

class PrintOpHandler(OpHandler):

    def handle(self, op):
        pprint(op)


class QueueHandler(OpHandler):

    def __init__(self, host=None, port=None, vhost=None, username=None,
                 password=None, exchange='oplog', queue='', dump=DUMP_JSON):
        super(QueueHandler, self).__init__()

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
        if all([f.is_valid(op['ns'], op['op']) for f in self.filters]):
            try:
                self.channel.basic_publish(
                    exchange=self.exchange, routing_key=self.queue, body=self.dump(op)
                )
            except BaseException as err:
                logging.error(err.message)


class _QueueWorker(threading.Thread):

    def __init__(self, q, stop, connection):
        super(_QueueWorker, self).__init__()
        self.connection = connection
        self.stop = stop
        self.q = q

    def run(self):
        while not self.stop.isSet():
            self.connection.handle(self.q.get())


class MultithreadedQueue(OpHandler):

    def __init__(self, *args, **kwargs):
        super(MultithreadedQueue, self).__init__()

        self.q = Queue.Queue()
        self.stop = threading.Event()
        self.tpool = {}

        threads = kwargs.pop('threads')
        for i in xrange(threads):
            self.tpool[i] = _QueueWorker(self.q, self.stop, QueueHandler(*args, **kwargs))
            self.tpool[i].name = "_QueueWorker_%s" % i
            self.tpool[i].daemon = True
            self.tpool[i].start()

    def handle(self, op):
        if all([f.is_valid(op['ns'], op['op']) for f in self.filters]):
            self.q.put(op)

    def close(self):
        self.q.join()
        self.stop.set()
