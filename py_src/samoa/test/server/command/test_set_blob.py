
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


class TestSetBlob(unittest.TestCase):

    def _build_fixture(self):
        """
        Builds a test-table with two partitions & three peers.

        'main', 'peer' each have a partition with preset value.
        'forwarder' has no partition.
        """

        common_fixture = ClusterStateFixture()
        self.table_uuid = UUID(
            common_fixture.add_table(
                data_type = DataType.BLOB_TYPE).uuid)

        self.cluster = PeeredCluster(common_fixture,
            server_names = ['main', 'peer', 'forwarder'])

        self.main_partition_uuid = self.cluster.add_partition(
            self.table_uuid, 'main')
        self.peer_partition_uuid = self.cluster.add_partition(
            self.table_uuid, 'peer')

        self.cluster.start_server_contexts()
        self.main_persister = self.cluster.persisters[self.main_partition_uuid]
        self.peer_persister = self.cluster.persisters[self.peer_partition_uuid]

        self.author_id = ClockUtil.generate_author_id()

        self.key = common_fixture.generate_bytes()
        self.preset_value = common_fixture.generate_bytes()
        self.new_value = common_fixture.generate_bytes()

        def populate():

            record = PersistedRecord()
            Blob.update(record, self.author_id, self.preset)
            yield self.main_persister.put(None, self.key, record)
            yield self.peer_persister.put(None, self.key, record)
            yield

        return populate

    def test_direct_get_blob_hit(self):
        populate = self._build_fixture()

    def test_direct_get_blob_miss(self):
        def test():
            yield self._build_fixture()

    def test_forwarded_get_blob(self):
        yield

    def test_direct_write_no_clock(self):
        def make_clock(author_id):
            return None

        populate = self._build_simple_fixture()
        self._simple_write_passes(populate, 'main', make_clock)

    def test_forwarded_write_no_clock(self):
        def make_clock(author_id):
            return None

        populate = self._build_simple_fixture()
        self._simple_write_passes(populate, 'forwarder', make_clock)

    def test_direct_write_with_clock(self):
        def make_clock(author_id):
            clock = ClusterClock()
            ClockUtil.tick(clock, author_id, None)
            return clock

        populate = self._build_simple_fixture()
        self._simple_write_passes(populate, 'main', make_clock)

    def test_forwarded_write_with_clock(self):
        def make_clock(author_id):
            clock = ClusterClock()
            ClockUtil.tick(clock, author_id, None)
            return clock

        populate = self._build_simple_fixture()
        self._simple_write_passes(populate, 'forwarder', make_clock)

    def test_direct_write_empty_clock(self):
        def make_clock(author_id):
            return ClusterClock()

        populate = self._build_simple_fixture()
        self._simple_write_fails(populate, 'main', make_clock)

    def test_forwarded_write_empty_clock(self):
        def make_clock(author_id):
            return ClusterClock()

        populate = self._build_simple_fixture()
        self._simple_write_fails(populate, 'forwarder', make_clock)

    def test_direct_write_wrong_clock(self):
        def make_clock(author_id):
            clock = ClusterClock()
            ClockUtil.tick(clock, ClockUtil.generate_author_id(), None)
            return clock 

        populate = self._build_simple_fixture()
        self._simple_write_fails(populate, 'main', make_clock)

    def test_forwarded_write_wrong_clock(self):
        def make_clock(author_id):
            clock = ClusterClock()
            ClockUtil.tick(clock, ClockUtil.generate_author_id(), None)
            return clock

        populate = self._build_simple_fixture()
        self._simple_write_fails(populate, 'forwarder', make_clock)

    def _simple_write_fails(self, populate, server_name, make_clock):

        def test():

            author_id = yield populate()
            response = yield self._make_request(server_name,
                make_clock(author_id))

            samoa_response = response.get_message()
            self.assertFalse(response.get_error_code())
            self.assertFalse(samoa_response.success)

            # preset value was returned
            self.assertEquals(response.get_response_data_blocks(),
                [self.preset])

            expected_clock = ClusterClock()
            ClockUtil.tick(expected_clock, author_id, None)

            # validate expected clock was returned
            self.assertEquals(ClockAncestry.CLOCKS_EQUAL, ClockUtil.compare(
                expected_clock, samoa_response.cluster_clock))

            response.finish_response()

            # preset record is unchanged
            record = yield self.persister.get(self.key)

            self.assertItemsEqual(record.blob_value, [self.preset])
            self.assertEquals(ClockAncestry.CLOCKS_EQUAL, ClockUtil.compare(
                expected_clock, record.cluster_clock))

            # cleanup
            self.cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run_test(test)

    def _simple_write_passes(self, populate, server_name, make_clock):

        def test():

            author_id = yield populate()
            response = yield self._make_request(server_name,
                make_clock(author_id))

            samoa_response = response.get_message()
            self.assertFalse(response.get_error_code())
            self.assertTrue(samoa_response.success)
            self.assertEquals(samoa_response.replication_success, 1)
            self.assertEquals(samoa_response.replication_failure, 0)
            response.finish_response()

            # validate written record
            record = yield self.persister.get(self.key)
            self.assertEquals(list(record.blob_value), [self.value])

            expected_clock = ClusterClock()
            ClockUtil.tick(expected_clock, author_id, None)
            ClockUtil.tick(expected_clock, author_id, None)

            self.assertEquals(ClockAncestry.CLOCKS_EQUAL, ClockUtil.compare(
                expected_clock, record.cluster_clock))

            # cleanup
            self.cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run_test(test)

    def test_set_blob_error_cases(self):
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

    def _make_request(self, server_name, request_clock = None):

        request = yield self.cluster.schedule_request(server_name)

        samoa_request = request.get_message()
        samoa_request.set_type(CommandType.SET_BLOB)
        samoa_request.set_table_uuid(self.table_uuid.to_bytes())
        samoa_request.set_key(self.key)

        if request_clock:
            samoa_request.mutable_cluster_clock().CopyFrom(request_clock)

        request.add_data_block(self.value)

        response = yield request.flush_request()
        yield response

