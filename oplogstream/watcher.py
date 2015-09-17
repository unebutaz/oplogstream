
from pymongo import MongoClient, ASCENDING
from pymongo.errors import AutoReconnect
from pymongo.cursor import CursorType
from time import sleep

import logging

class OplogWatcher(object):
    def __init__(self, handler, host='localhost', port=27017, db='local', collection='oplog.rs', timeout=1.0):
        self.handler = handler
        self.timeout = timeout
        self.host = host
        self.port = port
        self.db=db
        self.collection=collection

    def start(self):
        logging.info('Trying to connect to mongo.')
        connection = MongoClient(host=self.host, port=self.port, socketTimeoutMS=20000)
        logging.info('Connection established.')

        oplog = connection.get_database(self.db).get_collection(self.collection)
        options = {
            # 'filter': {{"ts": {"$gte": start}}
            'cursor_type': CursorType.TAILABLE_AWAIT,
            'no_cursor_timeout': True,
        }
        # todo: handle keyboard interrupt
        while True:
            cursor = oplog.find(**options).sort("$natural", ASCENDING)
            try:
                while cursor.alive:
                    try:
                        self.handler.handle(cursor.next())
                    except StopIteration:
                        sleep(self.timeout)
            except AutoReconnect:
                sleep(self.timeout)
            except KeyboardInterrupt:
                logging.info('Ctrl-C, stop iteration')
                break
