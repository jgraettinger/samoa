
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

class TestGetBlob(unittest.TestCase):

    def setUp(self):

        self.injector = TestModule().configure(getty.Injector())
        self.fixture = self.injector.get_instance(ClusterStateFixture)

    def test_get_blob(self):

        test_table = self.fixture.add_table(
            data_type = DataType.BLOB_TYPE)
        test_part = self.fixture.add_local_partition(test_table.uuid)

        listener = self.injector.get_instance(Listener)
        context = listener.get_context()

        def test():

            # query for runtime partition persister
            part = context.get_cluster_state().get_table_set().get_table(
                UUID(test_table.uuid)).get_partition(UUID(test_part.uuid))
            persister = part.get_persister()

            # set a test value
            persister.put(
                lambda cr, nr: nr.set_value('test-value') or 1,
                'a-test-key', 10)

            # issue blob get request (missing key)
            server = yield Server.connect_to(
                listener.get_address(), listener.get_port())
            request = yield server.schedule_request()

            request.get_message().set_type(CommandType.GET_BLOB)

            gb = request.get_message().mutable_get_blob()
            gb.set_table_uuid(test_table.uuid)
            gb.set_key('a-missing-key')

            response = yield request.finish_request()
            self.assertFalse(response.get_error_code())

            msg = response.get_message()
            self.assertEquals(len(msg.data_block_length), 0)
            self.assertFalse(msg.get_blob.found)

            response.finish_response()

            # issue blob get request (present key)
            server = yield Server.connect_to(
                listener.get_address(), listener.get_port())
            request = yield server.schedule_request()

            request.get_message().set_type(CommandType.GET_BLOB)

            gb = request.get_message().mutable_get_blob()
            gb.set_table_uuid(test_table.uuid)
            gb.set_key('a-test-key')

            response = yield request.finish_request()
            self.assertFalse(response.get_error_code())

            msg = response.get_message()
            self.assertEquals(len(msg.data_block_length), 1)

            value = yield response.read_interface().read_data(
                msg.data_block_length[0])
            self.assertEquals(value, 'test-value')

            response.finish_response()
            # cleanup
            context.get_tasklet_group().cancel_group()
            yield

        Proactor.get_proactor().run_test(test)

    def test_error_cases(self):

        table_no_partitions = self.fixture.add_table(
            data_type = DataType.BLOB_TYPE)

        table_remote_not_available = self.fixture.add_table(
            data_type = DataType.BLOB_TYPE)

        self.fixture.add_remote_partition(
            table_remote_not_available.uuid)

        listener = self.injector.get_instance(Listener)
        context = listener.get_context()

        def test():

            server = yield Server.connect_to(
                listener.get_address(), listener.get_port())

            # missing request message
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.GET_BLOB)

            response = yield request.finish_request()
            self.assertEquals(response.get_error_code(), 400)
            response.finish_response()

            # malformed table UUID 
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.GET_BLOB)

            dp = request.get_message().mutable_get_blob()
            dp.set_table_uuid('invalid uuid')
            dp.set_key('test-key')

            response = yield request.finish_request()
            self.assertEquals(response.get_error_code(), 400)
            response.finish_response()

            # non-existent table 
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.GET_BLOB)

            dp = request.get_message().mutable_get_blob()
            dp.set_table_uuid(UUID.from_random().to_hex())
            dp.set_key('test-key')

            response = yield request.finish_request()
            self.assertEquals(response.get_error_code(), 404)
            response.finish_response()

            # no table partitions
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.GET_BLOB)

            dp = request.get_message().mutable_get_blob()
            dp.set_table_uuid(table_no_partitions.uuid)
            dp.set_key('test-key')

            response = yield request.finish_request()
            self.assertEquals(response.get_error_code(), 404)
            response.finish_response()

            # remote partitions not available
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.GET_BLOB)

            dp = request.get_message().mutable_get_blob()
            dp.set_table_uuid(table_remote_not_available.uuid)
            dp.set_key('test-key')

            response = yield request.finish_request()
            self.assertEquals(response.get_error_code(), 503)
            response.finish_response()

            # cleanup
            context.get_tasklet_group().cancel_group()
            yield

        Proactor.get_proactor().run_test(test)

    def test_get_blob_forwarding(self):
        self.assertFalse(True)

