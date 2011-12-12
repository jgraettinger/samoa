
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

            peer A: has a partition with preset-value A
            peer B: has a partition with preset-value B
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

        self.part_A = self.cluster.add_partition(self.table_uuid, 'peer_A')
        self.part_B = self.cluster.add_partition(self.table_uuid, 'peer_B')
        self.part_nil = self.cluster.add_partition(self.table_uuid, 'peer_nil')

        self.cluster.start_server_contexts()
        self.persisters = self.cluster.persisters

        self.key = common_fixture.generate_bytes()
        self.value = common_fixture.generate_bytes()
        self.preset_A = common_fixture.generate_bytes()
        self.preset_B = common_fixture.generate_bytes()

        def populate():

            record = PersistedRecord()
            record.add_blob_value(self.preset_A)
            ClockUtil.tick(record.mutable_cluster_clock(), self.part_A)

            yield self.persisters[self.part_A].put(None, self.key, record)

            record = PersistedRecord()
            record.add_blob_value(self.preset_B)
            ClockUtil.tick(record.mutable_cluster_clock(), self.part_B)

            yield self.persisters[self.part_B].put(None, self.key, record)
            yield

        return populate


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

        self.partition_uuid = self.cluster.add_partition(
            self.table_uuid, 'main')

        self.cluster.start_server_contexts()
        self.persister = self.cluster.persisters[self.partition_uuid]

        self.key = common_fixture.generate_bytes()
        self.value = common_fixture.generate_bytes()
        self.preset = common_fixture.generate_bytes()

        def populate():

            # set preset value in main's persister
            record = PersistedRecord()
            record.add_blob_value(self.preset)
            ClockUtil.tick(record.mutable_cluster_clock(), self.partition_uuid)

            yield self.persister.put(None, self.key, record)
            yield

        return populate

    def test_direct_write_no_clock(self):
        populate = self._build_simple_fixture()
        self._simple_write_passes(populate, 'main', None)

    def test_forwarded_write_no_clock(self):
        populate = self._build_simple_fixture()
        self._simple_write_passes(populate, 'forwarder', None)

    def test_direct_write_with_clock(self):
        populate = self._build_simple_fixture()

        clock = ClusterClock()
        ClockUtil.tick(clock, self.partition_uuid)
        self._simple_write_passes(populate, 'main', clock)

    def test_forwarded_write_with_clock(self):
        populate = self._build_simple_fixture()

        clock = ClusterClock()
        ClockUtil.tick(clock, self.partition_uuid)
        self._simple_write_passes(populate, 'forwarder', clock)

    def test_direct_write_empty_clock(self):
        populate = self._build_simple_fixture()

        self._simple_write_fails(populate, 'main', ClusterClock())

    def test_forwarded_write_empty_clock(self):
        populate = self._build_simple_fixture()

        self._simple_write_fails(populate, 'forwarder', ClusterClock())

    def test_direct_write_wrong_clock(self):
        populate = self._build_simple_fixture()

        clock = ClusterClock()
        ClockUtil.tick(clock, UUID.from_random())
        self._simple_write_fails(populate, 'main', clock)
        
    def test_forwarded_write_wrong_clock(self):
        populate = self._build_simple_fixture()

        clock = ClusterClock()
        ClockUtil.tick(clock, UUID.from_random())
        self._simple_write_fails(populate, 'forwarder', clock)

    def _simple_write_fails(self, populate, server_name, clock):

        def test():

            yield populate()
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

            # preset record is unchanged
            record = yield self.persister.get(self.key)

            self.assertEquals(list(record.blob_value), [self.preset])
            self.assertEquals(ClockAncestry.EQUAL, ClockUtil.compare(
                expected_clock, record.cluster_clock))

            # cleanup
            self.cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run_test(test)

    def _simple_write_passes(self, populate, server_name, clock):

        def test():

            yield populate()
            response = yield self._make_request(server_name, clock)

            samoa_response = response.get_message()
            self.assertFalse(response.get_error_code())
            self.assertTrue(samoa_response.success)
            self.assertEquals(samoa_response.replication_success, 1)
            self.assertEquals(samoa_response.replication_failure, 0)
            response.finish_response()

            # validate written record
            expected_clock = ClusterClock()
            ClockUtil.tick(expected_clock, self.partition_uuid)
            ClockUtil.tick(expected_clock, self.partition_uuid)

            record = yield self.persister.get(self.key)

            self.assertEquals(list(record.blob_value), [self.value])
            self.assertEquals(ClockAncestry.EQUAL, ClockUtil.compare(
                expected_clock, record.cluster_clock))

            # cleanup
            self.cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run_test(test)


    def test_quorum_write_A(self):
        populate = self._build_diverged_fixture()

        clock = ClusterClock()
        ClockUtil.tick(clock, self.part_A)

        self._quorum_write_test(populate, 'peer_A',
            clock, [self.value, self.preset_B])

    def test_quorum_write_B(self):
        populate = self._build_diverged_fixture()

        clock = ClusterClock()
        ClockUtil.tick(clock, self.part_B)

        self._quorum_write_test(populate, 'peer_B',
            clock, [self.value, self.preset_A])

    def test_quorum_write_nil(self):
        populate = self._build_diverged_fixture()

        clock = ClusterClock() # empty

        self._quorum_write_test(populate, 'peer_nil',
            clock, [self.value, self.preset_A, self.preset_B])

    def _quorum_write_test(self, populate, server_name,
            request_clock, expected_values):

        def test():

            yield populate()
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

            server_partitions = {
                'peer_A': self.part_A,
                'peer_B': self.part_B,
                'peer_nil': self.part_nil,
            }

            expected_clock = ClusterClock()
            ClockUtil.tick(expected_clock, self.part_A)
            ClockUtil.tick(expected_clock, self.part_B)
            ClockUtil.tick(expected_clock, server_partitions[server_name])

            # assert all servers have expected values & clock
            for srv_name, part_uuid in server_partitions.items():

                record = yield self.persisters[part_uuid].get(self.key)

                self.assertEquals(set(record.blob_value), set(expected_values))
                self.assertEquals(ClockAncestry.EQUAL, ClockUtil.compare(
                    expected_clock, record.cluster_clock))

            # cleanup
            self.cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run_test([test, validate])

    def test_error_cases(self):
        populate = self._build_simple_fixture()

        def test():

            yield populate()

            # missing table
            request = yield self.cluster.schedule_request('forwarder')

            samoa_request = request.get_message()
            samoa_request.set_type(CommandType.SET_BLOB)
            samoa_request.set_key(self.key)

            request.add_data_block(self.value)

            response = yield request.flush_request()
            samoa_response = response.get_message()
            self.assertEquals(response.get_error_code(), 400)

            response.finish_response()

            # missing key
            request = yield self.cluster.schedule_request('forwarder')

            samoa_request = request.get_message()
            samoa_request.set_type(CommandType.SET_BLOB)
            samoa_request.set_table_uuid(self.table_uuid.to_bytes())

            request.add_data_block(self.value)

            response = yield request.flush_request()
            samoa_response = response.get_message()
            self.assertEquals(response.get_error_code(), 400)

            response.finish_response()

            # missing data-block
            request = yield self.cluster.schedule_request('forwarder')

            samoa_request = request.get_message()
            samoa_request.set_type(CommandType.SET_BLOB)
            samoa_request.set_table_uuid(self.table_uuid.to_bytes())
            samoa_request.set_key(self.key)

            response = yield request.flush_request()
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

        request.add_data_block(self.value)

        response = yield request.flush_request()
        yield response

