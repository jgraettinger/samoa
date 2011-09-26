
import getty
import unittest

from samoa.core.protobuf import CommandType
from samoa.core.proactor import Proactor
from samoa.server.listener import Listener
from samoa.server.command_handler import CommandHandler
from samoa.client.server import Server

from samoa.test.module import TestModule


class MuxTestHandler(CommandHandler):

    def __init__(self):
        self._pending = {}
        CommandHandler.__init__(self)

    def handle(self, rstate):

        mux_ids = [int(i) for i in rstate.get_request_data_blocks()]

        self._pending[mux_ids[0]] = rstate

        for trigger_id in mux_ids[1:]:
            self._pending[trigger_id].flush_client_response()
            del self._pending[trigger_id]

        yield

class TestRequestMuxing(unittest.TestCase):

    def setUp(self):

        self.injector = TestModule().configure(getty.Injector())
        self.listener = self.injector.get_instance(Listener)
        self.listener.get_protocol().set_command_handler(
            CommandType.TEST, MuxTestHandler())

        self.context = self.listener.get_context()

    def test_basic(self):

        futures = {}

        def test():

            server = yield Server.connect_to(
                self.listener.get_address(), self.listener.get_port())

            for i in xrange(20):

                request = yield server.schedule_request()
                request.get_message().set_type(CommandType.TEST)
                request.add_data_block(str(i))

                futures[i] = request.flush_request()

            print futures
            self.context.get_tasklet_group().cancel_group()
            yield

        Proactor.get_proactor().run_test(test)

