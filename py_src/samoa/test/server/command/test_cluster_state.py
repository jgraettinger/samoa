
import getty
import unittest

from samoa.core.protobuf import CommandType, ClusterState
from samoa.core.uuid import UUID
from samoa.core.proactor import Proactor
from samoa.server.listener import Listener
from samoa.client.server import Server

from samoa.test.module import TestModule
from samoa.test.cluster_state_fixture import ClusterStateFixture

class TestClusterState(unittest.TestCase):

    def setUp(self):

        self.injector = TestModule().configure(getty.Injector())
        self.fixture = self.injector.get_instance(ClusterStateFixture)
        self.peer_fixture = ClusterStateFixture()

    def test_cluster_state(self):

        test_table = self.fixture.add_table().uuid
        peer_fixture = self.fixture.clone_peer()

        # local server & peer share common table, but divergent partitions
        test_local_part = self.fixture.add_local_partition(test_table)
        test_peer_part = peer_fixture.add_local_partition(test_table)

        listener = self.injector.get_instance(Listener)
        context = listener.get_context()

        def test():

            server = yield Server.connect_to(
                listener.get_address(), listener.get_port())

            # issue cluster-state request _without_ request state
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.CLUSTER_STATE)

            response = yield request.flush_request()
            self.assertFalse(response.get_error_code())

            response_state = ClusterState()
            response_state.ParseFromBytes(
                response.get_response_data_blocks()[0])

            response.finish_response()

            # verify state reflects the local server
            result_table = response_state.table[0]
            self.assertEquals(result_table.uuid, test_table)
            self.assertEquals(len(result_table.partition), 1)
            self.assertEquals(result_table.partition[0].uuid,
                test_local_part.uuid)

            # issue cluster-state request _with_ peer request state
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.CLUSTER_STATE)

            request.add_data_block(peer_fixture.state.SerializeToBytes())

            response = yield request.flush_request()
            self.assertFalse(response.get_error_code())

            response_state = ClusterState()
            response_state.ParseFromBytes(
                response.get_response_data_blocks()[0])

            response.finish_response()

            # verify state reflects merging of peer & local server
            result_table = response_state.table[0]
            self.assertEquals(result_table.uuid, test_table)
            self.assertEquals(len(result_table.partition), 2)

            # cleanup
            context.get_tasklet_group().cancel_group()
            yield

        Proactor.get_proactor().run_test(test)

