
import getty
import unittest

from samoa.core.protobuf import CommandType, PersistedRecord, ClusterClock
from samoa.core.uuid import UUID
from samoa.core.proactor import Proactor
from samoa.datamodel.data_type import DataType
from samoa.datamodel.clock_util import ClockUtil, ClockAncestry
from samoa.datamodel.blob import Blob
from samoa.persistence.persister import Persister

from samoa.test.module import TestModule
from samoa.test.peered_cluster import PeeredCluster
from samoa.test.cluster_state_fixture import ClusterStateFixture


class TestReplication(unittest.TestCase):

    def _build_fixture(self):
        """
        Builds a test-table with replication-factor 4, and five peers:

            peer A: has a partition with preset-value A
            peer B: has a partition with preset-value B
            peer nil: has a partition, but no value
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
            server_names = ['peer_A', 'peer_B', 'peer_nil'])

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

            author_A = self.cluster.contexts['peer_A'].get_cluster_state(
                ).get_table_set(
                ).get_table(self.table_uuid
                ).get_partition(self.part_A
                ).get_author_id()

            record = PersistedRecord()
            Blob.update(record, author_A, self.preset_A)
            yield self.persisters[self.part_A].put(None, self.key, record)

            author_B = self.cluster.contexts['peer_B'].get_cluster_state(
                ).get_table_set(
                ).get_table(self.table_uuid
                ).get_partition(self.part_B
                ).get_author_id()

            record = PersistedRecord()
            Blob.update(record, author_B, self.preset_B)
            yield self.persisters[self.part_B].put(None, self.key, record)

            yield author_A, author_B

        return populate

    def test_quorum_read_A(self):
        populate = self._build_fixture()
        self._replicated_read_test(populate, 'peer_A')

    def test_quorum_read_nil(self):
        populate = self._build_fixture()
        self._replicated_read_test(populate, 'peer_nil')

    def _replicated_read_test(self, populate, server_name):
        """
        Regardless of the node issued to, quorum-reads in this fixture
        should always produce the same (merged) result
        """

        def test():

            author_A, author_B = yield populate()
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
            self.assertItemsEqual(response.get_response_data_blocks(),
                [self.preset_A, self.preset_B])

            expected_clock = ClusterClock()
            ClockUtil.tick(expected_clock, author_A, None)
            ClockUtil.tick(expected_clock, author_B, None)

            # ensure expected clock is present in response
            self.assertEquals(ClockAncestry.CLOCKS_EQUAL, ClockUtil.compare(
                expected_clock, samoa_response.cluster_clock))

            response.finish_response()

            # cleanup
            self.cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run_test(test)

    def test_simple_write_A(self):
        populate = self._build_fixture()
        self._replicated_write_test(populate, 'peer_A',
            1, 1, 0, [self.value, self.preset_B])

    def test_simple_write_nil(self):
        populate = self._build_fixture()
        self._replicated_write_test(populate, 'peer_nil',
            1, 1, 0, [self.value, self.preset_A, self.preset_B])

    def test_quorum_write_A(self):
        populate = self._build_fixture()
        self._replicated_write_test(populate, 'peer_A',
            4, 3, 1, [self.value, self.preset_B])

    def test_quorum_write_nil(self):
        populate = self._build_fixture()
        self._replicated_write_test(populate, 'peer_nil',
            4, 3, 1, [self.value, self.preset_A, self.preset_B])

    def _replicated_write_test(self, populate, server_name, quorum,
            expected_success, expected_failure, expected_values):

        def test():

            author_A, author_B = yield populate()

            request = yield self.cluster.schedule_request(server_name)

            samoa_request = request.get_message()
            samoa_request.set_type(CommandType.SET_BLOB)
            samoa_request.set_table_uuid(self.table_uuid.to_bytes())
            samoa_request.set_key(self.key)
            samoa_request.set_requested_quorum(quorum)

            request.add_data_block(self.value)
            response = yield request.flush_request()

            samoa_response = response.get_message()
            self.assertFalse(response.get_error_code())
            self.assertTrue(samoa_response.success)

            self.assertEquals(expected_success,
                samoa_response.replication_success)
            self.assertEquals(expected_failure,
                samoa_response.replication_failure)

            self.assertEquals(len(samoa_response.data_block_length), 0)

            response.finish_response()
            yield

        def validate():

            # assert all servers have expected values
            for part_uuid in [self.part_A, self.part_B, self.part_nil]:

                record = yield self.persisters[part_uuid].get(self.key)
                self.assertItemsEqual(record.blob_value, expected_values)

            # cleanup
            self.cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run_test([test, validate])

