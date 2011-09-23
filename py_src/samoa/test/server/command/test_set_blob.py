
import getty
import unittest

from samoa.core.protobuf import CommandType, PersistedRecord, ClusterClock
from samoa.core.uuid import UUID
from samoa.core.proactor import Proactor
from samoa.client.server import Server
from samoa.server.listener import Listener
from samoa.datamodel.data_type import DataType
from samoa.datamodel.clock_util import ClockUtil, ClockAncestry
from samoa.persistence.persister import Persister

from samoa.test.module import TestModule
from samoa.test.peered_cluster import PeeredCluster
from samoa.test.cluster_state_fixture import ClusterStateFixture


class TestSetBlob(unittest.TestCase):

    def _build_diverged_fixture(self):
        """
        Builds a test-table with replication-factor 4, and five peers:

            peer A: has a partition with test-value A
            peer B: has a partition with test-value B
            peer nil: has a partition, but no value
            forwarder: has no partition
            unreachable: a remote partition which is unavailable
        """

        common_fixture = ClusterStateFixture()
        self.table_uuid = UUID(
            common_fixture.add_table(
                data_type = DataType.BLOB_TYPE,
                replication_factor = 4).uuid)

        # add a partition owned by an unreachable peer
        common_fixture.add_remote_partition(self.table_uuid)

        self.cluster = PeeredCluster(common_fixture,
            server_names = ['peer_A', 'peer_B', 'peer_nil', 'forwarder'])

        # create local partitions for peer A, B, & nil
        self.partition_uuids = {}
        for srv_name in ['peer_A', 'peer_B', 'peer_nil']:
            self.partition_uuids[srv_name] = UUID(self.cluster.fixtures[
                srv_name].add_local_partition(self.table_uuid).uuid)

        self.cluster.start_server_contexts()

        # pull out top persister rolling-hash layers
        self.hashes = {}

        for srv_name in ['peer_A', 'peer_B', 'peer_nil']:
            self.hashes[srv_name] = \
                self.cluster.contexts[srv_name].get_cluster_state(
                    ).get_table_set(
                    ).get_table(self.table_uuid
                    ).get_partition(self.partition_uuids[srv_name]
                    ).get_persister(
                    ).get_layer(0)

        self.key = 'test-key' #common_fixture.generate_bytes()
        self.value = 'test-value' #common_fixture.generate_bytes()
        self.preset_A = 'preset-A' #common_fixture.generate_bytes()
        self.preset_B = 'preset-B' #common_fixture.generate_bytes()

        # set test key/value under peer A & B
        for (srv_name, test_value) in [
                ('peer_A', self.preset_A), ('peer_B', self.preset_B)]:

            record = PersistedRecord()
            record.add_blob_value(test_value)

            ClockUtil.tick(record.mutable_cluster_clock(),
                self.partition_uuids[srv_name])

            raw_record = self.hashes[srv_name].prepare_record(self.key,
                record.ByteSize())
            raw_record.set_value(record.SerializeToBytes())
            self.hashes[srv_name].commit_record()

    def _build_simple_fixture(self):
        """
        Builds a test-table with a single partition, and two peers:

        main: has a partition with preset value
        forwarder: has no partition
        """

        common_fixture = ClusterStateFixture()
        self.table_uuid = UUID(
            common_fixture.add_table(
                data_type = DataType.BLOB_TYPE).uuid)

        self.cluster = PeeredCluster(common_fixture,
            server_names = ['main', 'forwarder'])

        self.partition_uuid = UUID(self.cluster.fixtures[
            'main'].add_local_partition(self.table_uuid).uuid)

        self.cluster.start_server_contexts()

        # pull out main's persister
        self.main_hash = self.cluster.contexts['main'].get_cluster_state(
            ).get_table_set(
            ).get_table(self.table_uuid
            ).get_partition(self.partition_uuid
            ).get_persister(
            ).get_layer(0)

        self.key = common_fixture.generate_bytes()
        self.value = common_fixture.generate_bytes()
        self.preset = common_fixture.generate_bytes()

        # set preset value in main's persister
        record = PersistedRecord()
        record.add_blob_value(self.preset)

        ClockUtil.tick(record.mutable_cluster_clock(), self.partition_uuid)

        raw_record = self.main_hash.prepare_record(self.key,
            record.ByteSize())
        raw_record.set_value(record.SerializeToBytes())
        self.main_hash.commit_record()

    def test_direct_write_no_clock(self):
        self._build_simple_fixture()
        self._simple_write_passes('main', None)

    def test_forwarded_write_no_clock(self):
        self._build_simple_fixture()
        self._simple_write_passes('forwarder', None)

    def test_direct_write_with_clock(self):
        self._build_simple_fixture()

        clock = ClusterClock()
        ClockUtil.tick(clock, self.partition_uuid)
        self._simple_write_passes('main', clock)

    def test_forwarded_write_with_clock(self):
        self._build_simple_fixture()

        clock = ClusterClock()
        ClockUtil.tick(clock, self.partition_uuid)
        self._simple_write_passes('forwarder', clock)

    def test_direct_write_empty_clock(self):
        self._build_simple_fixture()

        self._simple_write_fails('main', ClusterClock())

    def test_forwarded_write_empty_clock(self):
        self._build_simple_fixture()

        self._simple_write_fails('forwarder', ClusterClock())

    def test_direct_write_wrong_clock(self):
        self._build_simple_fixture()

        clock = ClusterClock()
        ClockUtil.tick(clock, UUID.from_random())
        self._simple_write_fails('main', clock)
        
    def test_forwarded_write_wrong_clock(self):
        self._build_simple_fixture()

        clock = ClusterClock()
        ClockUtil.tick(clock, UUID.from_random())
        self._simple_write_fails('forwarder', clock)

    def _simple_write_fails(self, server_name, clock):

        def test():

            clock = ClusterClock()
            ClockUtil.tick(clock, UUID.from_random())

            response = yield self._make_request(server_name, clock)

            samoa_response = response.get_message()
            self.assertFalse(response.get_error_code())
            self.assertFalse(samoa_response.success)

            # preset value was returned
            self.assertEquals(response.get_response_data_blocks(),
                [self.preset])

            expected_clock = ClusterClock()
            ClockUtil.tick(expected_clock, self.partition_uuid)

            # expected clock was returned
            self.assertEquals(ClockAncestry.EQUAL, ClockUtil.compare(
                expected_clock, samoa_response.cluster_clock))

            response.finish_response()

            # cleanup
            self.cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run_test(test)

    def _simple_write_passes(self, server_name, clock):

        def test():

            response = yield self._make_request(server_name, clock)

            samoa_response = response.get_message()
            self.assertFalse(response.get_error_code())
            self.assertTrue(samoa_response.success)
            self.assertEquals(samoa_response.replication_success, 1)
            self.assertEquals(samoa_response.replication_failure, 0)
            response.finish_response()

            # cleanup
            self.cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run_test(test)

        # validate written record
        record = PersistedRecord()
        record.ParseFromBytes(self.main_hash.get(self.key).value)

        expected_clock = ClusterClock()
        ClockUtil.tick(expected_clock, self.partition_uuid)
        ClockUtil.tick(expected_clock, self.partition_uuid)

        self.assertEquals(list(record.blob_value), [self.value])
        self.assertEquals(ClockAncestry.EQUAL, ClockUtil.compare(
            expected_clock, record.cluster_clock))

    def test_quorum_write_A(self):
        self._build_diverged_fixture()

        clock = ClusterClock()
        ClockUtil.tick(clock, self.partition_uuids['peer_A'])

        self._quorum_write_test('peer_A', clock, [self.value, self.preset_B])

    def test_quorum_write_B(self):
        self._build_diverged_fixture()

        clock = ClusterClock()
        ClockUtil.tick(clock, self.partition_uuids['peer_B'])

        self._quorum_write_test('peer_B', clock, [self.value, self.preset_A])

    def test_quorum_write_nil(self):
        self._build_diverged_fixture()

        clock = ClusterClock() # empty

        self._quorum_write_test('peer_nil', clock,
            [self.value, self.preset_A, self.preset_B])

    def _quorum_write_test(self, server_name, request_clock, expected_values):

        def test():

            response = yield self._make_request(server_name, request_clock, 4)

            samoa_response = response.get_message()
            self.assertFalse(response.get_error_code())
            self.assertTrue(samoa_response.success)
            self.assertEquals(samoa_response.replication_success, 3)
            self.assertEquals(samoa_response.replication_failure, 1)

            self.assertEquals(len(samoa_response.data_block_length), 0)

            response.finish_response()
            yield

        def validate():

            expected_clock = ClusterClock()
            ClockUtil.tick(expected_clock, self.partition_uuids['peer_A'])
            ClockUtil.tick(expected_clock, self.partition_uuids['peer_B'])
            ClockUtil.tick(expected_clock, self.partition_uuids[server_name])

            # assert all servers have expected values & clock
            for srv_name in ['peer_A', 'peer_B', 'peer_nil']:

                record = PersistedRecord()
                record.ParseFromBytes(
                    self.hashes[srv_name].get(self.key).value)

                self.assertEquals(set(record.blob_value), set(expected_values))

                self.assertEquals(ClockAncestry.EQUAL, ClockUtil.compare(
                    expected_clock, record.cluster_clock))

            # cleanup
            self.cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run_test([test, validate])

    def test_error_cases(self):
        self._build_simple_fixture()

        def test():

            # missing table
            request = yield self.cluster.schedule_request('forwarder')

            samoa_request = request.get_message()
            samoa_request.set_type(CommandType.SET_BLOB)
            samoa_request.set_key(self.key)

            samoa_request.add_data_block_length(len(self.value))
            request.start_request()
            request.write_interface().queue_write(self.value)

            response = yield request.finish_request()
            samoa_response = response.get_message()
            self.assertEquals(response.get_error_code(), 400)

            response.finish_response()

            # missing key
            request = yield self.cluster.schedule_request('forwarder')

            samoa_request = request.get_message()
            samoa_request.set_type(CommandType.SET_BLOB)
            samoa_request.set_table_uuid(self.table_uuid.to_bytes())

            samoa_request.add_data_block_length(len(self.value))
            request.start_request()
            request.write_interface().queue_write(self.value)

            response = yield request.finish_request()
            samoa_response = response.get_message()
            self.assertEquals(response.get_error_code(), 400)

            response.finish_response()

            # missing data-block
            request = yield self.cluster.schedule_request('forwarder')

            samoa_request = request.get_message()
            samoa_request.set_type(CommandType.SET_BLOB)
            samoa_request.set_table_uuid(self.table_uuid.to_bytes())
            samoa_request.set_key(self.key)

            response = yield request.finish_request()
            samoa_response = response.get_message()
            self.assertEquals(response.get_error_code(), 400)

            response.finish_response()

            # cleanup
            self.cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run_test(test)

    def _make_request(self, server_name, request_clock = None, quorum = None):

        request = yield self.cluster.schedule_request(server_name)

        samoa_request = request.get_message()
        samoa_request.set_type(CommandType.SET_BLOB)
        samoa_request.set_table_uuid(self.table_uuid.to_bytes())
        samoa_request.set_key(self.key)

        if request_clock:
            samoa_request.mutable_cluster_clock().CopyFrom(request_clock)
        if quorum:
            samoa_request.set_requested_quorum(4)

        samoa_request.add_data_block_length(len(self.value))
        request.start_request()
        request.write_interface().queue_write(self.value)

        response = yield request.finish_request()
        yield response

