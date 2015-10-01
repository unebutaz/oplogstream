import unittest

from oplogstream.handlers import *

class HandlerTest(unittest.TestCase):

    def test_base_handler(self):
        with self.assertRaises(NotImplementedError):
            OpHandler().handle(None)

    def test_print_handler(self):
        handler = PrintOpHandler()
        self.assertIsInstance(handler, OpHandler)

    def test_queue_handler(self):
        handler = QueueHandler()
        self.assertIsInstance(handler, OpHandler)

if __name__ == '__main__':
    unittest.main()