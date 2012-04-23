import sys
import getty
import unittest

from samoa.core.protobuf import CommandType
from samoa.core.proactor import Proactor
from samoa.server.listener import Listener
from samoa.client.server import Server

from samoa.test.module import TestModule

class TestPing(unittest.TestCase):

    def setUp(self):

        self.injector = TestModule().configure(getty.Injector())

    def test_ping(self):

        listener = self.injector.get_instance(Listener)
        context = listener.get_context()

        def test():

            server = yield Server.connect_to(
                listener.get_address(), listener.get_port())

            # make a ping request to server
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.PING)
            request.get_message().set_key('test-key')
            request.add_data_block('hello, world')

            # receive ping response
            response = yield request.flush_request()
            self.assertFalse(response.get_error_code())
            response.finish_response()

            context.shutdown()
            yield

        Proactor.get_proactor().run(test())

