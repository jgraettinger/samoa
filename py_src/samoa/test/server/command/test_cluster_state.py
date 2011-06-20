
import getty
import unittest

from samoa.core.protobuf import CommandType
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

        test_table = self.fixture.add_table()
        peer_fixture = self.fixture.clone_peer()

        # local server & peer share common table, but divergent partitions
        test_local_part = self.fixture.add_local_partition(test_table)
        test_peer_part = peer_fixture.add_local_partition(
            peer_fixture.state.table[0])

        listener = self.injector.get_instance(Listener)
        context = listener.get_context()

        def test():

            server = yield Server.connect_to(
                listener.get_address(), listener.get_port())

            # issue cluster-state request _without_ request state
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.CLUSTER_STATE)

            response = yield request.finish_request()
            self.assertFalse(response.get_error_code())

            # verify state reflects the local server
            result_table = response.get_message().cluster_state.table[0]
            self.assertEquals(result_table.uuid, test_table.uuid)
            self.assertEquals(len(result_table.partition), 1)
            self.assertEquals(result_table.partition[0].uuid,
                test_local_part.uuid)

            response.finish_response()

            # issue cluster-state request _with_ peer request state
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.CLUSTER_STATE)
            request.get_message().mutable_cluster_state().CopyFrom(
                peer_fixture.state)

            response = yield request.finish_request()
            self.assertFalse(response.get_error_code())

            # verify state reflects merging of peer & local server
            result_table = response.get_message().cluster_state.table[0]
            self.assertEquals(result_table.uuid, test_table.uuid)
            self.assertEquals(len(result_table.partition), 2)

            response.finish_response()

            # cleanup
            server.close()
            listener.cancel()
            yield

        proactor = Proactor.get_proactor()
        proactor.spawn(test)
        proactor.run()

    def test_error_cases(self):

        test_table = self.fixture.add_table()
        peer_fixture = self.fixture.clone_peer()

        # local server & peer share common table, but divergent partitions
        test_local_part = self.fixture.add_local_partition(test_table)
        test_peer_part = peer_fixture.add_local_partition(
            peer_fixture.state.table[0])

        listener = self.injector.get_instance(Listener)
        context = listener.get_context()

        def test():

            server = yield Server.connect_to(
                listener.get_address(), listener.get_port())

            # issue cluster-state request with malformed peer state 
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.CLUSTER_STATE)
            request.get_message().mutable_cluster_state().CopyFrom(
                peer_fixture.state)

            request.get_message().cluster_state.table[0].set_uuid(
                'invalid-uuid')

            response = yield request.finish_request()
            self.assertEquals(response.get_error_code(), 406)
            response.finish_response()

            # cleanup
            server.close()
            listener.cancel()
            yield

        proactor = Proactor.get_proactor()
        proactor.spawn(test)
        proactor.run()
