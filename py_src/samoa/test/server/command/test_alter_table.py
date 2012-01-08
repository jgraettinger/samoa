
import getty
import unittest

from samoa.core.protobuf import CommandType
from samoa.core.uuid import UUID
from samoa.core.proactor import Proactor
from samoa.server.listener import Listener
from samoa.client.server import Server
from samoa.datamodel.data_type import DataType

from samoa.test.module import TestModule
from samoa.test.cluster_state_fixture import ClusterStateFixture

class TestAlterTable(unittest.TestCase):

    def setUp(self):

        self.injector = TestModule().configure(getty.Injector())
        self.fixture = self.injector.get_instance(ClusterStateFixture)

    def test_alter_table(self):

        test_table = self.fixture.add_table(name = 'test_table')
        test_table.set_replication_factor(1)

        test_table_uuid = UUID(test_table.uuid)

        for i in xrange(5):
            self.fixture.add_remote_partition(test_table_uuid)

        listener = self.injector.get_instance(Listener)
        context = listener.get_context()

        def test():

            server = yield Server.connect_to(
                listener.get_address(), listener.get_port())
            request = yield server.schedule_request()

            request.get_message().set_type(CommandType.ALTER_TABLE)
            request.get_message().set_table_uuid(test_table_uuid.to_bytes())

            t = request.get_message().mutable_alter_table()
            t.set_name('new_name')
            t.set_replication_factor(3)
            t.set_consistency_horizon(1234)

            response = yield request.flush_request()
            self.assertFalse(response.get_error_code())
            response.finish_response()

            table_set = context.get_cluster_state().get_table_set()

            # not available under old name
            self.assertFalse(table_set.get_table_by_name('test_table'))

            # but is present under new name
            table = table_set.get_table_by_name('new_name')

            self.assertEquals(table.get_uuid(), test_table_uuid)

            # replication factor / consistency horizon have been updated
            self.assertEquals(table.get_replication_factor(), 3)
            self.assertEquals(table.get_consistency_horizon(), 1234)

            context.shutdown()
            yield

        Proactor.get_proactor().run_test(test)

    def test_error_cases(self):

        test_table = self.fixture.add_table(name = 'test_table')
        listener = self.injector.get_instance(Listener)
        context = listener.get_context()

        def test():

            server = yield Server.connect_to(
                listener.get_address(), listener.get_port())

            # table name or UUID not set
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.ALTER_TABLE)

            ct = request.get_message().mutable_alter_table()

            response = yield request.flush_request()
            self.assertEquals(response.get_error_code(), 400)
            response.finish_response()

            # alter_table message not present
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.ALTER_TABLE)
            request.get_message().set_table_name(test_table.name)

            response = yield request.flush_request()
            self.assertEquals(response.get_error_code(), 400)
            response.finish_response()

            context.shutdown()
            yield

        Proactor.get_proactor().run_test(test)

