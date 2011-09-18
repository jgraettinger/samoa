
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

        test_table = UUID(self.fixture.add_table().uuid)
        test_part = UUID(self.fixture.add_local_partition(test_table).uuid)

        listener = self.injector.get_instance(Listener)
        context = listener.get_context()

        def test():

            # precondition: runtime partition exists on server
            table = context.get_cluster_state().get_table_set(
                ).get_table(test_table)
            part = table.get_partition(test_part)

            self.assertEquals(part.get_uuid(), test_part)

            # issue partition drop request
            server = yield Server.connect_to(
                listener.get_address(), listener.get_port())
            request = yield server.schedule_request()

            request.get_message().set_type(CommandType.DROP_PARTITION)
            request.get_message().set_table_uuid(test_table.to_bytes())
            request.get_message().set_partition_uuid(test_part.to_bytes())

            response = yield request.finish_request()
            self.assertFalse(response.get_error_code())
            response.finish_response()

            # postcondition: table is still live, but partition is not
            table = context.get_cluster_state().get_table_set(
                ).get_table(test_table)

            self.assertFalse(table.get_partition(test_part))

            # cleanup
            context.get_tasklet_group().cancel_group()
            yield

        Proactor.get_proactor().run_test(test)

    def test_error_cases(self):

        test_table = UUID(self.fixture.add_table().uuid)
        test_part = UUID(self.fixture.add_local_partition(test_table).uuid)

        listener = self.injector.get_instance(Listener)
        context = listener.get_context()

        def test():

            server = yield Server.connect_to(
                listener.get_address(), listener.get_port())

            # missing table 
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.DROP_PARTITION)
            request.get_message().set_partition_uuid(test_part.to_bytes())

            response = yield request.finish_request()
            self.assertEquals(response.get_error_code(), 400)
            response.finish_response()

            # missing partition
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.DROP_PARTITION)
            request.get_message().set_table_uuid(test_table.to_bytes())

            response = yield request.finish_request()
            self.assertEquals(response.get_error_code(), 400)
            response.finish_response()

            # cleanup
            context.get_tasklet_group().cancel_group()
            yield

        Proactor.get_proactor().run_test(test)

