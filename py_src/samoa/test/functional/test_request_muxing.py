
import getty
import functools
import unittest

from samoa.core.protobuf import CommandType
from samoa.core.proactor import Proactor
from samoa.server.listener import Listener
from samoa.server.command_handler import CommandHandler
from samoa.server.client import Client
from samoa.client.server import Server

from samoa.test.module import TestModule


class MuxTestHandler(CommandHandler):

    def __init__(self):
        self.pending = {}
        CommandHandler.__init__(self)

    def handle(self, rstate):

        # head is this request id, tail is previous requests to respond to
        mux_ids = [int(i) for i in rstate.get_request_data_blocks()]

        self.pending[mux_ids[0]] = rstate

        # send responses for the marked requests
        for mux_id in mux_ids[1:]:
            self.pending[mux_id].flush_response()
            del self.pending[mux_id]

        yield

class TestRequestMuxing(unittest.TestCase):

    def setUp(self):

        self.injector = TestModule().configure(getty.Injector())

        self.handler = MuxTestHandler()

        self.listener = self.injector.get_instance(Listener)
        self.listener.get_protocol().set_command_handler(
            CommandType.TEST, self.handler)

        self.context = self.listener.get_context()
        self.futures = {}
        self.server = None

    def _build_connection(self):
        self.server = yield Server.connect_to(
            self.listener.get_address(), self.listener.get_port())
        yield

    def _make_request(self, mux_id, mux_ids_to_release):

        request = yield self.server.schedule_request()
        request.get_message().set_type(CommandType.TEST)
        request.add_data_block(str(mux_id))

        for m_id in mux_ids_to_release:
            # ask server to respond to these previous requests
            request.add_data_block(str(m_id))

        self.futures[mux_id] = request.flush_request()
        yield

    def _validate_responses(self, mux_ids = []):

        # validate id's arrive in expected order
        for m_id in mux_ids:

            # Note: the test will hang here, if responses don't
            #  arrive in _exactly_ the expected order
            response = yield self.futures[m_id]
            response.finish_response()
            del self.futures[m_id]

        # assert no unexpected responses have arrived
        for future in self.futures.values():
            self.assertFalse(future.is_called())
        yield

    def test_basic(self):

        Proactor.get_proactor().run_test([
            self._build_connection,
            functools.partial(self._make_request, 1, []),
            functools.partial(self._make_request, 2, []),
            functools.partial(self._make_request, 3, [3, 1]),
            functools.partial(self._validate_responses, [3, 1]),
            functools.partial(self._make_request, 4, [2]),
            functools.partial(self._validate_responses, [2]),
            functools.partial(self._make_request, 5, []),
            functools.partial(self._make_request, 6, [6, 4, 5]),
            functools.partial(self._validate_responses, [6, 4, 5]),
            self.context.get_tasklet_group().cancel_group
        ])

    def test_concurrency_limit(self):

        # Client.max_request_concurrency requests, increasing id 
        request_order = range(Client.max_request_concurrency)

        # release in opposite order, except for 0
        release_order = list(reversed(request_order[1:]))

        def test():

            yield self._build_connection()

            # Client.max_request_concurrency initial requests
            for r_id in request_order:
                yield self._make_request(r_id, [])

            # A request to release all previous requests, except 0
            yield self._make_request(Client.max_request_concurrency,
                release_order)

            yield

        def validate():

            # Assert none of the requests were actually released
            self.assertEquals(len(self.handler.pending),
                Client.max_request_concurrency)

            # manually release the first request
            self.handler.pending[0].flush_response()
            del self.handler.pending[0]

            # Assert request 0 was released
            yield self._validate_responses([0])

            # Assert the remaining responses in release_order
            yield self._validate_responses(release_order)

            # cleanup
            self.context.get_tasklet_group().cancel_group()
            yield

        Proactor.get_proactor().run_test([test, validate])

