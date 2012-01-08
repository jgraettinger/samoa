
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

class TestCreateTable(unittest.TestCase):

    def setUp(self):

        self.injector = TestModule().configure(getty.Injector())
        self.fixture = self.injector.get_instance(ClusterStateFixture)

    def test_create_table(self):

        listener = self.injector.get_instance(Listener)
        context = listener.get_context()

        def test():

            server = yield Server.connect_to(
                listener.get_address(), listener.get_port())
            request = yield server.schedule_request()

            request.get_message().set_type(CommandType.CREATE_TABLE)

            ct = request.get_message().mutable_create_table()
            ct.set_name('test_table')
            ct.set_data_type(DataType.BLOB_TYPE.name)
            ct.set_replication_factor(3)
            ct.set_consistency_horizon(300)

            # extract created UUID from response
            response = yield request.flush_request()
            self.assertFalse(response.get_error_code())

            tbl_uuid = UUID(response.get_message().table_uuid)
            response.finish_response()

            # inspect server state's protobuf description
            server_state = context.get_cluster_state(
                ).get_protobuf_description()

            self.assertEquals(len(server_state.table), 1)
            self.assertEquals(server_state.table[0].name, 'test_table')
            self.assertEquals(server_state.table[0].replication_factor, 3)

            # runtime table can be queried by uuid and name
            table_set = context.get_cluster_state().get_table_set()

            table = table_set.get_table(tbl_uuid)

            self.assertEquals(table.get_uuid(),
                table_set.get_table_by_name('test_table').get_uuid())

            # runtime table has proper properties
            self.assertEquals(table.get_name(), 'test_table')
            self.assertEquals(table.get_data_type(), DataType.BLOB_TYPE)
            self.assertEquals(table.get_replication_factor(), 0)
            self.assertEquals(table.get_consistency_horizon(), 300)

            # cleanup
            context.shutdown()
            yield

        Proactor.get_proactor().run_test(test)

    def test_error_cases(self):

        self.fixture.add_table(name = 'existing_table')

        listener = self.injector.get_instance(Listener)
        context = listener.get_context()

        def test():

            server = yield Server.connect_to(
                listener.get_address(), listener.get_port())

            # name conflict
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.CREATE_TABLE)

            ct = request.get_message().mutable_create_table()
            ct.set_name('existing_table')
            ct.set_data_type(DataType.BLOB_TYPE.name)

            response = yield request.flush_request()
            self.assertEquals(response.get_error_code(), 409)
            response.finish_response()

            # invalid data type 
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.CREATE_TABLE)

            ct = request.get_message().mutable_create_table()
            ct.set_name('test_table')
            ct.set_data_type('INVALID')

            response = yield request.flush_request()
            self.assertEquals(response.get_error_code(), 406)
            response.finish_response()

            # missing create_table message
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.CREATE_TABLE)

            response = yield request.flush_request()
            self.assertEquals(response.get_error_code(), 400)
            response.finish_response()

            # cleanup
            context.shutdown()
            yield

        Proactor.get_proactor().run_test(test)

