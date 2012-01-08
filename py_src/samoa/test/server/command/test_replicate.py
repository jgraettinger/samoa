
import getty
import unittest

from samoa.core.protobuf import CommandType, PersistedRecord, ClusterClock
from samoa.core.uuid import UUID
from samoa.core.proactor import Proactor
from samoa.datamodel.data_type import DataType
from samoa.datamodel.clock_util import ClockUtil, ClockAncestry
from samoa.persistence.persister import Persister

from samoa.test.module import TestModule
from samoa.test.peered_cluster import PeeredCluster
from samoa.test.cluster_state_fixture import ClusterStateFixture

class TestReplicate(unittest.TestCase):

    def setUp(self):
        
        common_fixture = ClusterStateFixture(random_seed = 10)
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
            self.assertFalse(response.get_error_code())
            yield

        def validate():

            # key & value exist under peer_A only
            record = yield self.persisters[self.part_B].get(self.key)
            self.assertIsNone(record)

            record = yield self.persisters[self.part_A].get(self.key)
            self.assertEquals(record.blob_value[0], self.value_A)

            # cleanup
            self.cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run_test([test, validate])

    def test_forwarded_quorum_write(self):

        def test():

            response = yield self._make_request(False, True, 2)
            self.assertFalse(response.get_error_code())
            yield

        def validate():

            # key & value exist under both peers
            record = yield self.persisters[self.part_B].get(self.key)
            self.assertEquals(record.blob_value[0], self.value_A)

            record = yield self.persisters[self.part_A].get(self.key)
            self.assertEquals(record.blob_value[0], self.value_A)

            # cleanup
            self.cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run_test([test, validate])

    def _test_forwarded_reads(self):

        def test():

            import pdb; pdb.set_trace()

            print "part_A", self.part_A
            print "part_B", self.part_B

            for name in ['forwarder', 'peer_A', 'peer_B']:
            	print name, "CONTEXT"
                print self.cluster.contexts[name].get_cluster_state().get_protobuf_description()

            # create divergent fixtures under both peers
            record = PersistedRecord()
            record.add_blob_value(self.value_A)
            ClockUtil.tick(record.mutable_cluster_clock(), self.part_A)

            yield self.persisters[self.part_A].put(None, self.key, record)

            record = PersistedRecord()
            record.add_blob_value(self.value_B)
            ClockUtil.tick(record.mutable_cluster_clock(), self.part_B)

            yield self.persisters[self.part_B].put(None, self.key, record)

            # non-quorum read; will retrieve one or the other
            response = yield self._make_request(False, False, 1)

            record = PersistedRecord()
            record.ParseFromBytes(response.get_response_data_blocks()[0])

            self.assertEquals(len(record.blob_value), 1)
            self.assertIn(record.blob_value[0], [self.value_B, self.value_B])

            # cleanup
            self.cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run_test(test)


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
            record.add_blob_value(self.value_A)
            ClockUtil.tick(record.mutable_cluster_clock(), self.part_A)

            request.add_data_block(record.SerializeToBytes())

        response = yield request.flush_request()
        yield response

