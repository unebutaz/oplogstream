from pymongo import MongoClient, DESCENDING
from pymongo.errors import AutoReconnect
from pymongo.cursor import CursorType
from time import sleep

import logging

logger = logging.getLogger('__main__')

class OplogWatcher(object):

    def __init__(self, handler, dsn='mongodb://localhost', port=27017, replicaSet=None, db='local',
                 collection='oplog.rs', timeout=1.0):
        self.handler = handler
        self.timeout = timeout
        self.dsn = dsn
        self.port = int(port)
        self.db = db
        self.collection = collection
        self.replicaSet = replicaSet

    def start(self):
        logger.info('Trying to connect to mongo.')
        connection = MongoClient(self.dsn, port=self.port, replicaSet=self.replicaSet, socketTimeoutMS=20000)
        logger.info('Connection established.')

        oplog = connection.get_database(self.db).get_collection(self.collection)
        # todo: find latest message
        query = {}
        if oplog.count():
            query['ts'] = {"$gte": oplog.find().sort('$natural', DESCENDING).limit(-1).next()['ts']}
        else:
            logger.warning('Oplog is empty. Check connection parameters.')

        options = {
            'cursor_type': CursorType.TAILABLE_AWAIT,
            'no_cursor_timeout': True,
        }

        logger.info('Start following oplog.')
        # todo: handle keyboard interrupt (needed for foreground mode)
        while True:
            cursor = oplog.find(query, **options)
            try:
                while cursor.alive:
                    try:
                        self.handler.handle(cursor.next())
                    except StopIteration:
                        sleep(self.timeout)
            except AutoReconnect:
                sleep(self.timeout)
            except KeyboardInterrupt:
                logger.info('Ctrl-C, stop iteration')
                break
