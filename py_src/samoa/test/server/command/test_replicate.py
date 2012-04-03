
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

class TestReplicate(unittest.TestCase):

    def setUp(self):

        common_fixture = ClusterStateFixture()
        self.table_uuid = UUID(
            common_fixture.add_table(
                data_type = DataType.BLOB_TYPE,
                replication_factor = 2).uuid)

        self.cluster = PeeredCluster(common_fixture,
            server_names = ['forwarder', 'peer_A', 'peer_B'])

        self.part_A = self.cluster.add_partition(self.table_uuid, 'peer_A')
        self.part_B = self.cluster.add_partition(self.table_uuid, 'peer_B')

        self.key = common_fixture.generate_bytes()
        self.value_A = common_fixture.generate_bytes()
        self.value_B = common_fixture.generate_bytes()

        self.cluster.start_server_contexts()
        self.persisters = self.cluster.persisters
        return

    def test_direct_simple_write(self):

        def test():

            response = yield self._make_request(True, True, 1)
            response.finish_response()

            # key & value exist under peer_A only
            record = yield self.persisters[self.part_B].get(self.key)
            self.assertIsNone(record)

            record = yield self.persisters[self.part_A].get(self.key)
            self.assertEquals(record.blob_value[0], self.value_A)

            # cleanup
            self.cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run(test())

    def test_direct_quorum_write(self):

        def test():

            response = yield self._make_request(True, True, 2)
            response.finish_response()

            # key & value exist under both peers
            record = yield self.persisters[self.part_B].get(self.key)
            self.assertEquals(record.blob_value[0], self.value_A)

            record = yield self.persisters[self.part_A].get(self.key)
            self.assertEquals(record.blob_value[0], self.value_A)

            # cleanup
            self.cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run(test())

    def test_forwarded_quorum_write(self):

        def test():

            response = yield self._make_request(False, True, 2)
            response.finish_response()

            # key & value exist under both peers
            record = yield self.persisters[self.part_B].get(self.key)
            self.assertEquals(record.blob_value[0], self.value_A)

            record = yield self.persisters[self.part_A].get(self.key)
            self.assertEquals(record.blob_value[0], self.value_A)

            # cleanup
            self.cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run(test())

    def test_direct_simple_read(self):

        def test():
            yield self._set_value_fixtures()

            response = yield self._make_request(True, False, 1)

            record = PersistedRecord()
            record.ParseFromBytes(response.get_response_data_blocks()[0])
            response.finish_response()

            self.assertEquals(len(record.blob_value), 1)
            self.assertEquals(record.blob_value[0], self.value_A)

            # cleanup
            self.cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run(test())

    def test_direct_quorum_read(self):

        def test():
            yield self._set_value_fixtures()

            response = yield self._make_request(True, False, 2)

            record = PersistedRecord()
            record.ParseFromBytes(response.get_response_data_blocks()[0])
            response.finish_response()

            self.assertEquals(len(record.blob_value), 2)
            self.assertItemsEqual(record.blob_value,
                [self.value_A, self.value_B])

            # cleanup
            self.cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run(test())

    def test_forwarded_simple_read(self):

        def test():
            yield self._set_value_fixtures()

            response = yield self._make_request(False, False, 1)

            record = PersistedRecord()
            record.ParseFromBytes(response.get_response_data_blocks()[0])
            response.finish_response()

            self.assertEquals(len(record.blob_value), 1)
            self.assertTrue(record.blob_value[0] in \
                [self.value_A, self.value_B])

            # cleanup
            self.cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run(test())

    def test_forwarded_quorum_read(self):

        def test():
            yield self._set_value_fixtures()

            response = yield self._make_request(False, False, 2)

            record = PersistedRecord()
            record.ParseFromBytes(response.get_response_data_blocks()[0])
            response.finish_response()

            self.assertEquals(len(record.blob_value), 2)
            self.assertItemsEqual(record.blob_value,
                [self.value_A, self.value_B])

            # cleanup
            self.cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run(test())

    def _make_request(self, is_direct, is_write, quorum):

        if is_direct:
            request = yield self.cluster.schedule_request('peer_A')
        else:
        	request = yield self.cluster.schedule_request('forwarder')

        samoa_request = request.get_message()
        samoa_request.set_type(CommandType.REPLICATE)
        samoa_request.set_table_uuid(self.table_uuid.to_bytes())
        samoa_request.set_key(self.key)
        samoa_request.set_requested_quorum(quorum)

        if is_direct:
            samoa_request.set_partition_uuid(self.part_A.to_bytes())
            samoa_request.add_peer_partition_uuid(self.part_B.to_bytes())

        if is_write:
            record = PersistedRecord()
            Blob.update(record, ClockUtil.generate_author_id(), self.value_A)
            request.add_data_block(record.SerializeToBytes())

        response = yield request.flush_request()
        self.assertFalse(response.get_error_code())
        yield response

    def _set_value_fixtures(self):

        # create divergent fixtures under both peers
        record = PersistedRecord()
        Blob.update(record, ClockUtil.generate_author_id(), self.value_A)
        yield self.persisters[self.part_A].put(None, self.key, record)

        record = PersistedRecord()
        Blob.update(record, ClockUtil.generate_author_id(), self.value_B)
        yield self.persisters[self.part_B].put(None, self.key, record)

        yield

