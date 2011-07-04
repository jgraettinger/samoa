
import getty
import unittest

from samoa.core.protobuf import CommandType
from samoa.core.uuid import UUID
from samoa.core.proactor import Proactor
from samoa.server.listener import Listener
from samoa.client.server import Server

from samoa.test.module import TestModule
from samoa.test.cluster_state_fixture import ClusterStateFixture


class TestDropPartition(unittest.TestCase):

    def setUp(self):

        self.injector = TestModule().configure(getty.Injector())
        self.fixture = self.injector.get_instance(ClusterStateFixture)

    def test_drop_partition(self):

        test_table = self.fixture.add_table()
        test_part = self.fixture.add_local_partition(test_table.uuid)

        listener = self.injector.get_instance(Listener)
        context = listener.get_context()

        def test():

            # precondition: runtime partition exists on server
            table = context.get_cluster_state().get_table_set().get_table(
                UUID(test_table.uuid))
            part = table.get_partition(UUID(test_part.uuid))

            self.assertEquals(part.get_uuid(), UUID(test_part.uuid))

            # issue partition drop request
            server = yield Server.connect_to(
                listener.get_address(), listener.get_port())
            request = yield server.schedule_request()

            request.get_message().set_type(CommandType.DROP_PARTITION)

            dp = request.get_message().mutable_drop_partition()
            dp.set_table_uuid(test_table.uuid)
            dp.set_partition_uuid(test_part.uuid)

            response = yield request.finish_request()
            self.assertFalse(response.get_error_code())
            response.finish_response()

            # postcondition: table is still live, but partition is not
            table = context.get_cluster_state().get_table_set().get_table(
                UUID(test_table.uuid))
            part = table.get_partition(UUID(test_part.uuid))

            self.assertFalse(part)
            
            # cleanup
            context.get_tasklet_group().cancel_group()
            yield

        proactor = Proactor.get_proactor()
        proactor.spawn(test)
        proactor.run()

    def test_error_cases(self):

        test_table = self.fixture.add_table()
        test_part = self.fixture.add_local_partition(test_table.uuid)

        listener = self.injector.get_instance(Listener)
        context = listener.get_context()

        def test():

            server = yield Server.connect_to(
                listener.get_address(), listener.get_port())

            # non-existent table 
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.DROP_PARTITION)

            dp = request.get_message().mutable_drop_partition()
            dp.set_table_uuid(UUID.from_random().to_hex())
            dp.set_partition_uuid(test_part.uuid)

            response = yield request.finish_request()
            self.assertEquals(response.get_error_code(), 404)
            response.finish_response()

            # non-existent partition 
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.DROP_PARTITION)

            dp = request.get_message().mutable_drop_partition()
            dp.set_table_uuid(test_table.uuid)
            dp.set_partition_uuid(UUID.from_random().to_hex())

            response = yield request.finish_request()
            self.assertEquals(response.get_error_code(), 404)
            response.finish_response()

            # malformed table UUID 
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.DROP_PARTITION)

            dp = request.get_message().mutable_drop_partition()
            dp.set_table_uuid('invalid uuid')
            dp.set_partition_uuid(test_part.uuid)

            response = yield request.finish_request()
            self.assertEquals(response.get_error_code(), 400)
            response.finish_response()

            # malformed partition UUID 
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.DROP_PARTITION)

            dp = request.get_message().mutable_drop_partition()
            dp.set_table_uuid(test_part.uuid)
            dp.set_partition_uuid('invalid uuid')

            response = yield request.finish_request()
            self.assertEquals(response.get_error_code(), 400)
            response.finish_response()

            # missing request message
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.DROP_PARTITION)

            response = yield request.finish_request()
            self.assertEquals(response.get_error_code(), 400)
            response.finish_response()

            # cleanup
            context.get_tasklet_group().cancel_group()
            yield

        proactor = Proactor.get_proactor()
        proactor.spawn(test)
        proactor.run()

