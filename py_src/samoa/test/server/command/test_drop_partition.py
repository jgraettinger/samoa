
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

        self.table_uuid = UUID(self.fixture.add_table().uuid)

        self.remote_partition_uuid = UUID(
            self.fixture.add_remote_partition(self.table_uuid).uuid)
        self.local_partition_uuid = UUID(
            self.fixture.add_local_partition(self.table_uuid).uuid)

        self.listener = self.injector.get_instance(Listener)
        self.context = self.listener.get_context()

    def test_drop_remote_partition(self):

        def test():

            table = self.context.get_cluster_state().get_table_set(
                ).get_table(self.table_uuid)

            # precondition: runtime partition is query-able & in ring
            self.assertTrue(table.get_partition(self.remote_partition_uuid))
            self.assertItemsEqual(
                [self.remote_partition_uuid, self.local_partition_uuid],
                [p.get_uuid() for p in table.get_ring()])

            # issue partition drop request
            yield self._make_request(self.remote_partition_uuid)

            table = self.context.get_cluster_state().get_table_set(
                ).get_table(self.table_uuid)

            # postcondition: remote partition is neither query-able or in ring
            self.assertFalse(table.get_partition(self.remote_partition_uuid))
            self.assertItemsEqual(
                [self.local_partition_uuid],
                [p.get_uuid() for p in table.get_ring()])

            # cleanup
            self.context.get_tasklet_group().cancel_group()
            yield

        Proactor.get_proactor().run_test(test)

    def test_drop_local_partition(self):

        def test():

            table = self.context.get_cluster_state().get_table_set(
                ).get_table(self.table_uuid)

            # precondition: local partition is query-able & in ring
            self.assertTrue(table.get_partition(self.local_partition_uuid))
            self.assertItemsEqual(
                [self.remote_partition_uuid, self.local_partition_uuid],
                [p.get_uuid() for p in table.get_ring()])

            # issue partition drop request
            yield self._make_request(self.local_partition_uuid)

            table = self.context.get_cluster_state().get_table_set(
                ).get_table(self.table_uuid)

            # postcondition: local partition is query-able, but not in ring
            self.assertTrue(table.get_partition(self.local_partition_uuid))
            self.assertItemsEqual(
                [self.remote_partition_uuid],
                [p.get_uuid() for p in table.get_ring()])

            # cleanup
            self.context.get_tasklet_group().cancel_group()
            yield

        Proactor.get_proactor().run_test(test)

    def _make_request(self, partition_uuid):

        server = yield Server.connect_to(
            self.listener.get_address(), self.listener.get_port())
        request = yield server.schedule_request()

        request.get_message().set_type(CommandType.DROP_PARTITION)
        request.get_message().set_table_uuid(self.table_uuid.to_bytes())
        request.get_message().set_partition_uuid(partition_uuid.to_bytes())

        response = yield request.flush_request()
        self.assertFalse(response.get_error_code())
        response.finish_response()
        yield

    def test_error_cases(self):

        self.partition_uuid = UUID(
            self.fixture.add_local_partition(self.table_uuid).uuid)

        def test():

            server = yield Server.connect_to(
                self.listener.get_address(), self.listener.get_port())

            # missing table 
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.DROP_PARTITION)
            request.get_message().set_partition_uuid(
                self.partition_uuid.to_bytes())

            response = yield request.flush_request()
            self.assertEquals(response.get_error_code(), 400)
            response.finish_response()

            # missing partition
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.DROP_PARTITION)
            request.get_message().set_table_uuid(self.table_uuid.to_bytes())

            response = yield request.flush_request()
            self.assertEquals(response.get_error_code(), 400)
            response.finish_response()

            # cleanup
            self.context.get_tasklet_group().cancel_group()
            yield

        Proactor.get_proactor().run_test(test)

