import unittest

from oplogstream import handlers

class HandlerTest(unittest.TestCase):

    def test_base_handler(self):
        with self.assertRaises(NotImplementedError):
            handlers.OpHandler().handle(None)

    def test_print_handler(self):
        handler = handlers.PrintOpHandler()
        self.assertIsInstance(handler, handlers.OpHandler)

if __name__ == '__main__':
    unittest.main()