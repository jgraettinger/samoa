
import getty
import unittest

from samoa.core.protobuf import CommandType
from samoa.core.uuid import UUID
from samoa.core.proactor import Proactor
from samoa.server.listener import Listener
from samoa.client.server import Server
from samoa.persistence.persister import Persister

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
            request.get_message().set_table_name('test_table')

            cp = request.get_message().mutable_create_partition()
            cp.set_ring_position(1234567)

            rl = cp.add_ring_layer()
            rl.set_storage_size(1<<19)
            rl.set_index_size(1234)

            # extract created UUID from response
            response = yield request.flush_request()
            self.assertFalse(response.get_error_code())

            part_uuid = UUID(response.get_message().partition_uuid)
            response.finish_response()

            # assert partition is live on the server
            cluster_state = context.get_cluster_state()

            part = cluster_state.get_table_set().get_table(
                UUID.from_name('test_table')).get_partition(part_uuid)

            self.assertEquals(part.get_ring_position(), 1234567)

            ring_layer = part.get_persister().layer(0)
            self.assertEquals(ring_layer.region_size(), (1<<19))
            self.assertEquals(ring_layer.index_size(), 1234)

            # cleanup
            context.shutdown()
            yield

        Proactor.get_proactor().run(test())

    def test_error_cases(self):

        self.fixture.add_table(name = 'test_table',
            uuid = UUID.from_name('test_table'))

        listener = self.injector.get_instance(Listener)
        context = listener.get_context()

        def test():

            server = yield Server.connect_to(
                listener.get_address(), listener.get_port())

            # missing table 
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.CREATE_PARTITION)

            cp = request.get_message().mutable_create_partition()
            cp.set_ring_position(1234567)

            rl = cp.add_ring_layer()
            rl.set_storage_size(1<<19)
            rl.set_index_size(1234)

            response = yield request.flush_request()
            self.assertEquals(response.get_error_code(), 400)
            response.finish_response()

            # missing create_partition message
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.CREATE_PARTITION)
            request.get_message().set_table_uuid(
                UUID.from_name('test_table').to_bytes())

            response = yield request.flush_request()
            self.assertEquals(response.get_error_code(), 400)
            response.finish_response()

            # missing a ring_layer
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.CREATE_PARTITION)
            request.get_message().set_table_uuid(
                UUID.from_name('test_table').to_bytes())

            cp = request.get_message().mutable_create_partition()
            cp.set_ring_position(1234567)

            response = yield request.flush_request()
            self.assertEquals(response.get_error_code(), 400)
            response.finish_response()

            # cleanup
            context.shutdown()
            yield

        Proactor.get_proactor().run(test())

