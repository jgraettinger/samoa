
import getty
import unittest

from samoa.core.protobuf import CommandType, PersistedRecord, ClusterClock
from samoa.core.uuid import UUID
from samoa.core.proactor import Proactor
from samoa.server.listener import Listener
from samoa.client.server import Server
from samoa.datamodel.data_type import DataType
from samoa.datamodel.clock_util import ClockUtil, ClockAncestry
from samoa.persistence.persister import Persister

from samoa.test.module import TestModule
from samoa.test.peered_cluster import PeeredCluster
from samoa.test.cluster_state_fixture import ClusterStateFixture


class TestGetBlob(unittest.TestCase):

    def _build_fixture(self):
        """
        Builds a test-table with replication-factor 4, and five peers:

            peer A: has a partition with test-value A
            peer B: has a partition with test-value B
            peer nil: has a partition, but no value
            forwarder: has no partition
            unreachable: a remote partition which is unavailable
        """

        server_names = ['peer_A', 'peer_B', 'peer_nil', 'forwarder']
        common_fixture = ClusterStateFixture()

        self.table_uuid = UUID(
            common_fixture.add_table(
                data_type = DataType.BLOB_TYPE,
                replication_factor = 4).uuid)

        # add a partition owned by an unreachable peer
        common_fixture.add_remote_partition(self.table_uuid)

        self.cluster = PeeredCluster(common_fixture,
            server_names = server_names)

        # create local partitions for peer A, B, & nil
        self.partition_uuids = {}
        for srv_name in ['peer_A', 'peer_B', 'peer_nil']:
            self.partition_uuids[srv_name] = UUID(self.cluster.fixtures[
                srv_name].add_local_partition(self.table_uuid).uuid)

        self.cluster.start_server_contexts()

        self.key = common_fixture.generate_bytes()
        self.value_A = common_fixture.generate_bytes()
        self.value_B = common_fixture.generate_bytes()

        # set test key/value under peer A & B
        for (srv_name, test_value) in [
                ('peer_A', self.value_A), ('peer_B', self.value_B)]:

            record_hash = self.cluster.contexts[srv_name].get_cluster_state(
                ).get_table_set(
                ).get_table(self.table_uuid
                ).get_partition(self.partition_uuids[srv_name]
                ).get_persister(
                ).get_layer(0)

            record = PersistedRecord()
            record.add_blob_value(test_value)

            ClockUtil.tick(record.mutable_cluster_clock(),
                self.partition_uuids[srv_name])

            raw_record = record_hash.prepare_record(self.key,
                record.ByteSize())
            raw_record.set_value(record.SerializeToBytes())
            record_hash.commit_record()

    def _quorum_read_test(self, server_name):
        """
        Regardless of the node issued to, quorum-reads of three in this fixture
        should always produce the same (merged) result
        """
        self._build_fixture()

        def test():

            request = yield self.cluster.schedule_request(server_name)

            samoa_request = request.get_message()
            samoa_request.set_type(CommandType.GET_BLOB)
            samoa_request.set_table_uuid(self.table_uuid.to_bytes())
            samoa_request.set_key(self.key)
            samoa_request.set_requested_quorum(4)

            response = yield request.flush_request()
            samoa_response = response.get_message()
            self.assertFalse(response.get_error_code())
            self.assertEquals(samoa_response.replication_success, 3)
            self.assertEquals(samoa_response.replication_failure, 1)

            self.assertEquals(len(samoa_response.data_block_length), 2)
            self.assertEquals(set(response.get_response_data_blocks()),
                set([self.value_A, self.value_B]))

            expected_clock = ClusterClock()
            ClockUtil.tick(expected_clock, self.partition_uuids['peer_A'])
            ClockUtil.tick(expected_clock, self.partition_uuids['peer_B'])

            # ensure the expected clock is present in the response
            self.assertEquals(ClockAncestry.EQUAL, ClockUtil.compare(
                expected_clock, samoa_response.cluster_clock))

            response.finish_response()

            # cleanup
            self.cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run_test(test)

    def test_quorum_read_A(self):
        self._quorum_read_test('peer_A')

    def test_quorum_read_nil(self):
        self._quorum_read_test('peer_nil')

    def test_quorum_read_B(self):
        self._quorum_read_test('peer_B')

    def test_quorum_read_forwarder(self):
        self._quorum_read_test('forwarder')

    def test_simple_read_A(self):

        self._build_fixture()

        def test():

            request = yield self.cluster.schedule_request('peer_A')

            samoa_request = request.get_message()
            samoa_request.set_type(CommandType.GET_BLOB)
            samoa_request.set_table_uuid(self.table_uuid.to_bytes())
            samoa_request.set_key(self.key)

            response = yield request.flush_request()
            samoa_response = response.get_message()
            self.assertFalse(response.get_error_code())
            self.assertEquals(samoa_response.replication_success, 1)
            self.assertEquals(samoa_response.replication_failure, 0)

            self.assertEquals(len(samoa_response.data_block_length), 1)
            self.assertEquals(response.get_response_data_blocks(),
                [self.value_A])

            expected_clock = ClusterClock()
            ClockUtil.tick(expected_clock, self.partition_uuids['peer_A'])

            # ensure the expected clock is present in the response
            self.assertEquals(ClockAncestry.EQUAL, ClockUtil.compare(
                expected_clock, samoa_response.cluster_clock))

            response.finish_response()

            # cleanup
            self.cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run_test(test)

    def test_simple_read_nil(self):

        self._build_fixture()

        def test():

            request = yield self.cluster.schedule_request('peer_nil')

            samoa_request = request.get_message()
            samoa_request.set_type(CommandType.GET_BLOB)
            samoa_request.set_table_uuid(self.table_uuid.to_bytes())
            samoa_request.set_key(self.key)

            response = yield request.flush_request()
            samoa_response = response.get_message()
            self.assertFalse(response.get_error_code())
            self.assertEquals(samoa_response.replication_success, 1)
            self.assertEquals(samoa_response.replication_failure, 0)

            self.assertEquals(len(samoa_response.data_block_length), 0)
            self.assertEquals(0,
                len(samoa_response.cluster_clock.partition_clock))

            response.finish_response()

            # cleanup
            self.cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run_test(test)

    def test_error_cases(self):

        self._build_fixture()

        def test():

            # missing table
            request = yield self.cluster.schedule_request('forwarder')

            samoa_request = request.get_message()
            samoa_request.set_type(CommandType.GET_BLOB)
            samoa_request.set_key(self.key)

            response = yield request.flush_request()
            samoa_response = response.get_message()
            self.assertEquals(response.get_error_code(), 400)

            response.finish_response()

            # missing key
            request = yield self.cluster.schedule_request('forwarder')

            samoa_request = request.get_message()
            samoa_request.set_type(CommandType.GET_BLOB)
            samoa_request.set_table_uuid(self.table_uuid.to_bytes())

            response = yield request.flush_request()
            samoa_response = response.get_message()
            self.assertEquals(response.get_error_code(), 400)

            response.finish_response()

            # cleanup
            self.cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run_test(test)

