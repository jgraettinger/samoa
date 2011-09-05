
import getty
import unittest

from samoa.core.protobuf import CommandType, PersistedRecord, ClusterClock
from samoa.core.uuid import UUID
from samoa.core.proactor import Proactor
from samoa.client.server import Server
from samoa.server.listener import Listener
from samoa.persistence.data_type import DataType
from samoa.datamodel.clock_util import ClockUtil, ClockAncestry

from samoa.test.module import TestModule
from samoa.test.peered_cluster import PeeredCluster
from samoa.test.cluster_state_fixture import ClusterStateFixture


class TestReplicateBlob(unittest.TestCase):

    def setUp(self):

        common_fixture = ClusterStateFixture()
        self.table_uuid = UUID(
            common_fixture.add_table(
                data_type = DataType.BLOB_TYPE,
                replication_factor = 2,
            ).uuid)

        self.cluster = PeeredCluster(common_fixture,
            server_names = ['local', 'remote'])

        # add a partition to both local & remote servers
        self.local_part_uuid = UUID(
            self.cluster.fixtures['local'].add_local_partition(
                self.table_uuid).uuid)

        self.remote_part_uuid = UUID(
            self.cluster.fixtures['remote'].add_local_partition(
                self.table_uuid).uuid)

        self.cluster.start_server_contexts()

        self.local_hash = self.cluster.contexts['local'].get_cluster_state(
            ).get_table_set(
            ).get_table(self.table_uuid
            ).get_partition(self.local_part_uuid
            ).get_persister(
            ).get_layer(0)

        self.remote_hash = self.cluster.contexts['remote'].get_cluster_state(
            ).get_table_set(
            ).get_table(self.table_uuid
            ).get_partition(self.remote_part_uuid
            ).get_persister(
            ).get_layer(0)

        self.key = common_fixture.generate_bytes()

    def test_new_key(self):

        expected = PersistedRecord()
        expected.add_blob_value('test-blob')

        ClockUtil.tick(expected.mutable_cluster_clock(), UUID.from_random())
        ClockUtil.tick(expected.mutable_cluster_clock(), UUID.from_random())

        def test():

            response = yield self._make_request(expected)

            # request succeeded
            self.assertFalse(response.get_error_code())

            # remote is updated but non-divergent
            self.assertTrue(response.get_message().replication.updated)
            self.assertFalse(response.get_message().replication.divergent)

            response.finish_response()

            # cleanup
            self.cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run_test(test)

        # validate exact record contents were stored
        self.assertEquals(expected.SerializeToBytes(),
            self.remote_hash.get(self.key).value)

    def test_merge_no_clocks(self):

        # server presumes it's copy is consistent
        self._merge_test(None, None, 'no_change')

    def test_merge_no_local_clock(self):

        remote_clock = ClusterClock()
        ClockUtil.tick(remote_clock, UUID.from_random())

        self._merge_test(None, remote_clock, 'remote')

    def test_merge_no_remote_clock(self):

        local_clock = ClusterClock()
        ClockUtil.tick(local_clock, UUID.from_random())

        self._merge_test(local_clock, None, 'local')

    def test_merge_clocks_equal(self):

        A = UUID.from_random()

        local_clock = ClusterClock()
        ClockUtil.tick(local_clock, A)

        remote_clock = ClusterClock()
        ClockUtil.tick(remote_clock, A)

        self._merge_test(local_clock, remote_clock, 'no_change')

    def test_merge_local_more_recent(self):

        A = UUID.from_random()

        local_clock = ClusterClock()
        ClockUtil.tick(local_clock, A)
        ClockUtil.tick(local_clock, A)

        remote_clock = ClusterClock()
        ClockUtil.tick(remote_clock, A)

        self._merge_test(local_clock, remote_clock, 'local')

    def test_merge_remote_more_recent(self):

        A = UUID.from_random()

        local_clock = ClusterClock()
        ClockUtil.tick(local_clock, A)

        remote_clock = ClusterClock()
        ClockUtil.tick(remote_clock, A)
        ClockUtil.tick(remote_clock, A)

        self._merge_test(local_clock, remote_clock, 'remote')

    def test_merge_clocks_diverge(self):

        local_clock = ClusterClock()
        ClockUtil.tick(local_clock, UUID.from_random())

        remote_clock = ClusterClock()
        ClockUtil.tick(remote_clock, UUID.from_random())

        self._merge_test(local_clock, remote_clock, 'both')

    def _merge_test(self, local_clock, remote_clock, expect):

        # set the local record fixture
        local_record = PersistedRecord()
        local_record.add_blob_value('local')

        if local_clock:
            local_record.mutable_cluster_clock().CopyFrom(local_clock)

        raw_record = self.local_hash.prepare_record(self.key,
            local_record.ByteSize())
        raw_record.set_value(local_record.SerializeToBytes())
        self.local_hash.commit_record()

        # set the remote record fixture
        remote_record = PersistedRecord()
        remote_record.add_blob_value('remote')

        if remote_clock:
            remote_record.mutable_cluster_clock().CopyFrom(remote_clock)

        raw_record = self.remote_hash.prepare_record(self.key,
            remote_record.ByteSize())
        raw_record.set_value(remote_record.SerializeToBytes())
        self.remote_hash.commit_record()

        def test():

            response = yield self._make_request(local_record)

            # request succeeded
            self.assertFalse(response.get_error_code())

            repl_resp = response.get_message().replication

            if expect == 'no_change':
                self.assertFalse(repl_resp.updated)
                self.assertFalse(repl_resp.divergent)
            elif expect == 'local':
                self.assertTrue(repl_resp.updated)
                self.assertFalse(repl_resp.divergent)
            elif expect == 'remote':
                self.assertFalse(repl_resp.updated)
                self.assertTrue(repl_resp.divergent)
            elif expect == 'both':
                self.assertTrue(repl_resp.updated)
                self.assertTrue(repl_resp.divergent)

            response.finish_response()

            # cleanup
            self.cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run_test(test)

        # extract local & remote stored records directly from hashes
        local_record = PersistedRecord()
        local_record.ParseFromBytes(
            self.local_hash.get(self.key).value)

        remote_record = PersistedRecord()
        remote_record.ParseFromBytes(
            self.remote_hash.get(self.key).value)

        if expect == 'no_change':
            # we manufactured equal clocks, such that local & remote
            #  store their original (actually divergent) blob values

            self.assertEquals(len(local_record.blob_value), 1)
            self.assertEquals(local_record.blob_value[0], 'local')

            self.assertEquals(len(remote_record.blob_value), 1)
            self.assertEquals(remote_record.blob_value[0], 'remote')

        elif expect == 'local':

            self.assertEquals(len(local_record.blob_value), 1)
            self.assertEquals(local_record.blob_value[0], 'local')

            self.assertEquals(len(remote_record.blob_value), 1)
            self.assertEquals(remote_record.blob_value[0], 'local')

        elif expect == 'remote':

            self.assertEquals(len(local_record.blob_value), 1)
            self.assertEquals(local_record.blob_value[0], 'remote')

            self.assertEquals(len(remote_record.blob_value), 1)
            self.assertEquals(remote_record.blob_value[0], 'remote')

        else:
            assert expect == 'both'

            self.assertEquals(len(local_record.blob_value), 2)
            self.assertEquals(local_record.blob_value[0], 'remote')
            self.assertEquals(local_record.blob_value[1], 'local')

            self.assertEquals(len(remote_record.blob_value), 2)
            self.assertEquals(remote_record.blob_value[0], 'remote')
            self.assertEquals(remote_record.blob_value[1], 'local')

    def _make_request(self, record):

        request = yield self.cluster.schedule_request('remote')
        request.get_message().set_type(CommandType.REPLICATE_BLOB)

        repl_req = request.get_message().mutable_replication()
        repl_req.set_table_uuid(self.table_uuid.to_bytes())
        repl_req.set_key(self.key)
        repl_req.set_source_partition_uuid(self.local_part_uuid.to_bytes())
        repl_req.set_target_partition_uuid(self.remote_part_uuid.to_bytes())

        record = record.SerializeToBytes()

        request.get_message().add_data_block_length(len(record))
        request.start_request()
        request.write_interface().queue_write(record)

        yield (yield request.finish_request())

