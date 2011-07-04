
import getty
import unittest

from samoa.core.protobuf import CommandType
from samoa.core.uuid import UUID
from samoa.core.proactor import Proactor
from samoa.server.listener import Listener
from samoa.client.server import Server

from samoa.test.module import TestModule
from samoa.test.cluster_state_fixture import ClusterStateFixture


class TestCreatePartition(unittest.TestCase):

    def setUp(self):

        self.injector = TestModule().configure(getty.Injector())
        self.fixture = self.injector.get_instance(ClusterStateFixture)

    def test_create_partition(self):

        self.fixture.add_table(name = 'test_table',
            uuid = UUID.from_name('test_table'))

        listener = self.injector.get_instance(Listener)
        context = listener.get_context()

        def test():

            server = yield Server.connect_to(
                listener.get_address(), listener.get_port())

            # create a partition
            request = yield server.schedule_request()

            request.get_message().set_type(CommandType.CREATE_PARTITION)

            cp = request.get_message().mutable_create_partition()
            cp.set_table_uuid(UUID.from_name('test_table').to_hex())
            cp.set_ring_position(1234567)
            cp.set_storage_size(1<<20)
            cp.set_index_size(1<<12)

            # extract created UUID from response
            response = yield request.finish_request()
            self.assertFalse(response.get_error_code())

            part_uuid = UUID(response.get_message(
                ).create_partition.partition_uuid)
            response.finish_response()

            # assert partition is live on the server
            cluster_state = context.get_cluster_state()

            part = cluster_state.get_table_set().get_table(
                UUID.from_name('test_table')).get_partition(part_uuid)

            self.assertEquals(part.get_ring_position(), 1234567)
            #self.assertEquals(part.get_storage_size(), 1 << 20)
            #self.assertEquals(part.get_index_size(), 1 << 12)

            # cleanup
            context.get_tasklet_group().cancel_tasklets()
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

            # non-existent table 
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.CREATE_PARTITION)

            cp = request.get_message().mutable_create_partition()
            cp.set_table_uuid(UUID.from_name('unknown_table').to_hex())
            cp.set_ring_position(1234567)
            cp.set_storage_size(1<<20)
            cp.set_index_size(1<<12)

            response = yield request.finish_request()
            self.assertEquals(response.get_error_code(), 404)
            response.finish_response()

            # malformed UUID 
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.CREATE_PARTITION)

            cp = request.get_message().mutable_create_partition()
            cp.set_table_uuid('invalid uuid')
            cp.set_ring_position(1234567)
            cp.set_storage_size(1<<20)
            cp.set_index_size(1<<12)

            response = yield request.finish_request()
            self.assertEquals(response.get_error_code(), 400)
            response.finish_response()

            # cleanup
            context.get_tasklet_group().cancel_tasklets()
            yield

        proactor = Proactor.get_proactor()
        proactor.spawn(test)
        proactor.run()

