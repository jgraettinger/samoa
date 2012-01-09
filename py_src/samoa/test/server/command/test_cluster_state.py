
import getty
import unittest

from samoa.core.protobuf import CommandType, ClusterState
from samoa.core.uuid import UUID
from samoa.core.proactor import Proactor
from samoa.server.listener import Listener
from samoa.client.server import Server

from samoa.test.module import TestModule
from samoa.test.peered_cluster import PeeredCluster
from samoa.test.cluster_state_fixture import ClusterStateFixture

class TestClusterState(unittest.TestCase):

    def _build_simple_fixture(self):

        common_fixture = ClusterStateFixture()
        self.table_uuid = UUID(
            common_fixture.add_table().uuid)

        self.cluster = PeeredCluster(common_fixture,
            server_names = ['peer_A', 'peer_B'],
            set_peers = False)

        # divergent partitions
        self.part_A = self.cluster.add_partition(self.table_uuid, 'peer_A')
        self.part_B = self.cluster.add_partition(self.table_uuid, 'peer_B')

        self.cluster.start_server_contexts()

    def test_simple_no_request_state(self):
        self._build_simple_fixture()

        def test():

            request = yield self.cluster.schedule_request('peer_A')

            samoa_request = request.get_message()
            samoa_request.set_type(CommandType.CLUSTER_STATE)
            response = yield request.flush_request()

            self.assertFalse(response.get_error_code())

            # parse cluster state response data block
            response_state = ClusterState()
            response_state.ParseFromBytes(
                response.get_response_data_blocks()[0])

            response.finish_response()

            # verify only partition B is present
            self.assertEquals(len(response_state.table), 1)
            self.assertEquals(len(response_state.table[0].partition), 1)
            self.assertEquals(self.part_A,
                UUID(response_state.table[0].partition[0].uuid))

            self.cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run_test(test)

    def test_simple_with_request_state(self):
        self._build_simple_fixture()

        def test():

            request = yield self.cluster.schedule_request('peer_A')

            samoa_request = request.get_message()
            samoa_request.set_type(CommandType.CLUSTER_STATE)

            request.add_data_block(
                self.cluster.contexts['peer_B'].get_cluster_state(
                    ).get_protobuf_description().SerializeToBytes())
            response = yield request.flush_request()

            self.assertFalse(response.get_error_code())

            # parse cluster state response data block
            response_state = ClusterState()
            response_state.ParseFromBytes(
                response.get_response_data_blocks()[0])

            response.finish_response()

            # verify state reflects merging of peer A & B
            self.assertEquals(len(response_state.table), 1)
            self.assertEquals(len(response_state.table[0].partition), 2)
            self.assertItemsEqual([self.part_A, self.part_B],
                [UUID(p.uuid) for p in response_state.table[0].partition])

            self.cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run_test(test)

