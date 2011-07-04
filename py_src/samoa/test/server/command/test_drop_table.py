
import getty
import unittest

from samoa.core.protobuf import CommandType
from samoa.core.uuid import UUID
from samoa.core.proactor import Proactor
from samoa.server.listener import Listener
from samoa.client.server import Server

from samoa.test.module import TestModule
from samoa.test.cluster_state_fixture import ClusterStateFixture


class TestDropTable(unittest.TestCase):

    def setUp(self):

        self.injector = TestModule().configure(getty.Injector())
        self.fixture = self.injector.get_instance(ClusterStateFixture)

    def test_drop_table(self):

        self.fixture.add_table(name = 'test_table',
            uuid = UUID.from_name('test_table'))

        listener = self.injector.get_instance(Listener)
        context = listener.get_context()

        def test():

            # precondition: runtime table exists on server
            table = context.get_cluster_state().get_table_set().get_table(
                UUID.from_name('test_table'))
            self.assertEquals(table.get_name(), 'test_table')

            # issue table drop request
            server = yield Server.connect_to(
                listener.get_address(), listener.get_port())
            request = yield server.schedule_request()

            request.get_message().set_type(CommandType.DROP_TABLE)

            dt = request.get_message().mutable_drop_table()
            dt.set_table_uuid(UUID.from_name('test_table').to_hex())

            response = yield request.finish_request()
            self.assertFalse(response.get_error_code())
            response.finish_response()

            # postcondition: table is no longer on server
            table = context.get_cluster_state().get_table_set().get_table(
                UUID.from_name('test_table'))
            self.assertFalse(table) 

            # cleanup
            context.get_tasklet_group().cancel_group()
            yield

        proactor = Proactor.get_proactor()
        proactor.spawn(test)
        proactor.run()

    def test_error_cases(self):

        self.fixture.add_table(name = 'test_table',
            uuid = UUID.from_name('test_table'))

        listener = self.injector.get_instance(Listener)
        context = listener.get_context()

        def test():

            server = yield Server.connect_to(
                listener.get_address(), listener.get_port())

            # table doesn't exist
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.DROP_TABLE)

            dt = request.get_message().mutable_drop_table()
            dt.set_table_uuid(UUID.from_random().to_hex())

            response = yield request.finish_request()
            self.assertEquals(response.get_error_code(), 404)
            response.finish_response()

            # malformed UUID
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.DROP_TABLE)

            dt = request.get_message().mutable_drop_table()
            dt.set_table_uuid('invalid uuid')

            response = yield request.finish_request()
            self.assertEquals(response.get_error_code(), 400)
            response.finish_response()

            # missing drop_table message
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.DROP_TABLE)

            response = yield request.finish_request()
            self.assertEquals(response.get_error_code(), 400)
            response.finish_response()

            # cleanup
            context.get_tasklet_group().cancel_group()
            yield

        proactor = Proactor.get_proactor()
        proactor.spawn(test)
        proactor.run()

