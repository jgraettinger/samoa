
import getty
import unittest

from samoa.core.protobuf import CommandType, PersistedRecord, ClusterClock
from samoa.core.uuid import UUID
from samoa.core.proactor import Proactor
from samoa.datamodel.data_type import DataType
from samoa.datamodel.clock_util import ClockUtil, ClockAncestry
from samoa.datamodel.blob import Blob

from samoa.test.module import TestModule
from samoa.test.peered_cluster import PeeredCluster
from samoa.test.cluster_state_fixture import ClusterStateFixture


class TestGetBlob(unittest.TestCase):

    def _build_fixture(self):
        """
        Builds a test-table with two partitions & three peers.

        'main', 'peer' each have a partition with preset value.
        'forwarder' has no partition.
        """

        common_fixture = ClusterStateFixture()
        self.table_uuid = UUID(
            common_fixture.add_table(
                data_type = DataType.BLOB_TYPE,
                replication_factor = 2).uuid)

        self.cluster = PeeredCluster(common_fixture,
            server_names = ['main', 'peer', 'forwarder'])

        # make peers explicitly known to forwarder; otherwise, it's
        #  a race as to whether discovery takes 2 or 1 iteration
        self.cluster.set_known_peer('forwarder', 'main')
        self.cluster.set_known_peer('forwarder', 'peer') 

        self.main_partition_uuid = self.cluster.add_partition(
            self.table_uuid, 'main')
        self.peer_partition_uuid = self.cluster.add_partition(
            self.table_uuid, 'peer')

        self.cluster.start_server_contexts()
        self.main_persister = self.cluster.persisters[self.main_partition_uuid]
        self.peer_persister = self.cluster.persisters[self.peer_partition_uuid]

        self.key = common_fixture.generate_bytes()
        self.preset_value = common_fixture.generate_bytes()

        def populate():

            record = PersistedRecord()
            Blob.update(record, ClockUtil.generate_author_id(), self.preset_value)
            yield self.main_persister.put(None, self.key, record)
            yield self.peer_persister.put(None, self.key, record)

            self.expected_clock = ClusterClock(record.cluster_clock)
            yield

        return populate

    def test_direct_hit(self):
        populate = self._build_fixture()
        def test():
            yield populate()

            yield self._validate_response_hit(
                (yield self._make_request('main', self.key)))

            self.cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run_test(test)

    def test_direct_miss(self):
        populate = self._build_fixture()
        def test():
            yield populate()

            yield self._validate_response_miss(
                (yield self._make_request('main', 'missing-key')))

            self.cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run_test(test)

    def test_forwarded(self):
        populate = self._build_fixture()
        def test():
            yield populate()

            yield self._validate_response_hit(
                (yield self._make_request('forwarder', self.key)))

            self.cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run_test(test)

    def test_quorum_hit(self):
        populate = self._build_fixture()
        def test():
            yield populate()

            response = yield self._make_request('main', self.key, 2)

            samoa_response = response.get_message()
            self.assertEquals(samoa_response.replication_success, 2)
            self.assertEquals(samoa_response.replication_failure, 0)
            yield self._validate_response_hit(response)

            self.cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run_test(test)

    def test_forwarded_quorum_miss(self):
        populate = self._build_fixture()
        def test():
            yield populate()

            response = yield self._make_request(
                'forwarder', 'missing-key', 2)

            samoa_response = response.get_message()
            self.assertEquals(samoa_response.replication_success, 2)
            self.assertEquals(samoa_response.replication_failure, 0)
            yield self._validate_response_miss(response)

            self.cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run_test(test)

    def test_error_cases(self):
        populate = self._build_fixture()
        def test():
            yield populate()

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

    def _validate_response_hit(self, response, value = None):
        value = value or self.preset_value

        samoa_response = response.get_message()
        self.assertItemsEqual(response.get_response_data_blocks(), [value])

        self.assertEquals(ClockAncestry.CLOCKS_EQUAL, ClockUtil.compare(
            self.expected_clock, samoa_response.cluster_clock))

        response.finish_response()
        yield

    def _validate_response_miss(self, response):

        samoa_response = response.get_message()
        self.assertFalse(samoa_response.success)

        self.assertItemsEqual(response.get_response_data_blocks(), [])

        expected_clock = ClusterClock()
        expected_clock.set_clock_is_pruned(False)

        self.assertEquals(ClockAncestry.CLOCKS_EQUAL, ClockUtil.compare(
            expected_clock, samoa_response.cluster_clock))
        yield

    def _make_request(self, server_name, key, quorum = 1):

        request = yield self.cluster.schedule_request(server_name)

        samoa_request = request.get_message()
        samoa_request.set_type(CommandType.GET_BLOB)
        samoa_request.set_table_uuid(self.table_uuid.to_bytes())
        samoa_request.set_key(key)
        samoa_request.set_requested_quorum(quorum)

        response = yield request.flush_request()
        self.assertFalse(response.get_error_code())
        yield response

