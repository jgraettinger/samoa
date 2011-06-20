
import getty
import unittest

from samoa.core.protobuf import CommandType
from samoa.core.uuid import UUID
from samoa.core.proactor import Proactor
from samoa.server.listener import Listener
from samoa.client.server import Server
from samoa.persistence.data_type import DataType

from samoa.test.module import TestModule
from samoa.test.cluster_state_fixture import ClusterStateFixture

class TestAlterTable(unittest.TestCase):

    def setUp(self):

        self.injector = TestModule().configure(getty.Injector())
        self.fixture = self.injector.get_instance(ClusterStateFixture)

    def test_alter_table(self):

        test_table = self.fixture.add_table(name = 'test_table')
        test_table.set_replication_factor(1)

        listener = self.injector.get_instance(Listener)
        context = listener.get_context()

        def test():

            server = yield Server.connect_to(
                listener.get_address(), listener.get_port())
            request = yield server.schedule_request()

            request.get_message().set_type(CommandType.ALTER_TABLE)

            t = request.get_message().mutable_alter_table()
            t.set_table_uuid(test_table.uuid)
            t.set_name('new_name')
            t.set_replication_factor(10)

            response = yield request.finish_request()
            self.assertFalse(response.get_error_code())
            response.finish_response()

            table_set = context.get_cluster_state().get_table_set()

            # not available under old name
            self.assertFalse(table_set.get_table_by_name('test_table'))

            # but is present under new name
            table = table_set.get_table_by_name('new_name')

            self.assertEquals(table.get_uuid(), UUID(test_table.uuid))

            # replication factor has been updated
            self.assertEquals(table.get_replication_factor(), 10)

            server.close()
            listener.cancel()
            yield

        proactor = Proactor.get_proactor()
        proactor.spawn(test)
        proactor.run()

    def test_error_cases(self):

        listener = self.injector.get_instance(Listener)
        context = listener.get_context()

        def test():

            server = yield Server.connect_to(
                listener.get_address(), listener.get_port())

            # table doesn't exist 
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.ALTER_TABLE)

            ct = request.get_message().mutable_alter_table()
            ct.set_table_uuid(UUID.from_random().to_hex())

            response = yield request.finish_request()
            self.assertEquals(response.get_error_code(), 404)
            response.finish_response()

            # malformed UUID
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.ALTER_TABLE)

            ct = request.get_message().mutable_alter_table()
            ct.set_table_uuid("invalid uuid")

            response = yield request.finish_request()
            self.assertEquals(response.get_error_code(), 400)
            response.finish_response()

            # alter_table message not present
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.ALTER_TABLE)

            response = yield request.finish_request()
            self.assertEquals(response.get_error_code(), 400)
            response.finish_response()

            server.close()
            listener.cancel()
            yield

        proactor = Proactor.get_proactor()
        proactor.spawn(test)
        proactor.run()

