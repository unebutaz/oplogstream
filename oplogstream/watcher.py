from pymongo.errors import AutoReconnect
from pymongo.cursor import CursorType
from time import sleep


class OplogWatcher(object):
    def __init__(self, handler, connection, timeout=1.0):
        self.handler = handler
        self.timeout = timeout
        self.connection = connection

    def start(self):
        options = {
            'cursor_type': CursorType.TAILABLE_AWAIT,
            'no_cursor_timeout': True,

        }
        while True:
            cursor = self.connection.find(**options)
            while cursor.alive:
                try:
                    self.handler.handle(cursor.next())
                except (StopIteration, AutoReconnect):
                    sleep(self.timeout)
