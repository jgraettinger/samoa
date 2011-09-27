
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

        mux_ids = [int(i) for i in rstate.get_request_data_blocks()]

        self.pending[mux_ids[0]] = rstate

        print "READ REQUEST", mux_ids[0]

        for trigger_id in mux_ids[1:]:
            self.pending[trigger_id].flush_client_response()
            del self.pending[trigger_id]

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

    def _make_request(self, future_id, respond_ids = [], assert_ids = []):

        # validate asserted id's arrive in expected order
        for f_id in assert_ids:

            # yield future to obtain the response
            # Note: the test will hang here, if responses don't
            #  arrive in _exactly_ the expected order
            response = yield self.futures[f_id]
            response.finish_response()
            del self.futures[f_id]

            print "GOT RESPONSE %d" % f_id

        for future in self.futures.values():
            self.assertFalse(future.is_called())

        if future_id is None:
            # this call was purely for validation; don't make a request
            yield

        request = yield self.server.schedule_request()
        request.get_message().set_type(CommandType.TEST)
        request.add_data_block(str(future_id))

        for resp_id in respond_ids:
            # instruct server to respond to these requests
            request.add_data_block(str(resp_id))

        self.futures[future_id] = request.flush_request()
        yield

    def test_basic(self):

        Proactor.get_proactor().run_test([
            self._build_connection,
            functools.partial(self._make_request, 1, [], []),
            functools.partial(self._make_request, 2, [], []),
            functools.partial(self._make_request, 3, [3, 1], []),
            functools.partial(self._make_request, 4, [2], [3, 1]),
            functools.partial(self._make_request, 5, [], [2]),
            functools.partial(self._make_request, 6, [6, 4, 5], []),
            functools.partial(self._make_request, None, None, [6, 4, 5]),
            self.context.get_tasklet_group().cancel_group
        ])

    def test_concurrency_limit(self):

        # make Client.max_request_concurrency requests, increasing id 
        request_order = range(Client.max_request_concurrency)

        # release in opposite order, except for 0
        release_order = list(reversed(request_order[1:]))

        def test():

            yield self._build_connection()

            for r_id in request_order:
                yield self._make_request(r_id, [], [])

            yield self._make_request(Client.max_request_concurrency,
                release_order, [])

            yield

        def validate():

            # Assert none of the requests were actually released
            self.assertEquals(len(self.handler.pending),
                Client.max_request_concurrency)

            # manually release the first request
            self.handler.pending[0].flush_client_response()
            del self.handler.pending[0]

            print "asserting 0 was released"
            # Assert request 0 was released
            yield self._make_request(None, None, [0])


            print "asserting rest were released", release_order
            # Assert the remaining responses in release_order
            yield self._make_request(None, None, release_order)

            # cleanup
            self.context.get_tasklet_group().cancel_group()
            yield

        Proactor.get_proactor().run_test([test, validate])

