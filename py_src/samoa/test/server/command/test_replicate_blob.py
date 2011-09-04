
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
from samoa.test.cluster_state_fixture import ClusterStateFixture


class TestReplicateBlob(unittest.TestCase):

    def setUp(self):

        self.injector = TestModule().configure(getty.Injector())
        self.fixture = self.injector.get_instance(ClusterStateFixture)

        self.table = self.fixture.add_table(
            data_type = DataType.BLOB_TYPE)
        self.partition = self.fixture.add_local_partition(self.table.uuid)

        self.listener = self.injector.get_instance(Listener)
        self.context = self.listener.get_context()

        self.key = self.fixture.generate_bytes()

        self.rolling_hash = self.context.get_cluster_state(
            ).get_table_set(
            ).get_table(UUID(self.table.uuid)
            ).get_partition(UUID(self.partition.uuid)
            ).get_persister(
            ).get_layer(0)

    def test_new_key(self):

        expected = PersistedRecord()
        expected.add_blob_value('test-blob')

        ClockUtil.tick(expected.mutable_cluster_clock(), UUID.from_random())
        ClockUtil.tick(expected.mutable_cluster_clock(), UUID.from_random())

        def test():

            response = yield self._make_request(expected)

            # request succeeded
            self.assertFalse(response.get_error_code())
            response.finish_response()

            # cleanup
            self.context.get_tasklet_group().cancel_group()
            yield

        Proactor.get_proactor().run_test(test)

        # exact record contents were stored
        self.assertEquals(expected.SerializeToBytes(),
            self.rolling_hash.get(self.key).value)

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

        raw_record = self.rolling_hash.prepare_record(self.key,
            local_record.ByteSize())
        raw_record.set_value(local_record.SerializeToBytes())
        self.rolling_hash.commit_record()

        # build the remote, replicated record
        remote_record = PersistedRecord()
        remote_record.add_blob_value('remote')

        if remote_clock:
            remote_record.mutable_cluster_clock().CopyFrom(remote_clock)

        response_record = PersistedRecord()

        def test():

            response = yield self._make_request(remote_record)

            # request succeeded
            self.assertFalse(response.get_error_code())

            # parse out a response record, if one was returned
            if len(response.get_response_data_blocks()) == 1:
                response_record.ParseFromBytes(
                    response.get_response_data_blocks()[0])

            response.finish_response()

            # cleanup
            self.context.get_tasklet_group().cancel_group()
            yield

        Proactor.get_proactor().run_test(test)

        stored_record = PersistedRecord()
        stored_record.ParseFromBytes(
            self.rolling_hash.get(self.key).value)

        if expect == 'no_change':
            self.assertEquals(len(stored_record.blob_value), 1)
            self.assertEquals(stored_record.blob_value[0], 'local')

            # remote up-to-date, no record was returned
            self.assertEquals(len(response_record.blob_value), 0)

        elif expect == 'local':
            self.assertEquals(len(stored_record.blob_value), 1)
            self.assertEquals(stored_record.blob_value[0], 'local')

            # remote out-of-date, local record was returned
            self.assertEquals(len(response_record.blob_value), 1)
            self.assertEquals(response_record.blob_value[0], 'local')

        elif expect == 'remote':
            self.assertEquals(len(stored_record.blob_value), 1)
            self.assertEquals(stored_record.blob_value[0], 'remote')

            # remote up-to-date, no record was returned
            self.assertEquals(len(response_record.blob_value), 0)

        else:
            assert expect == 'both'

            self.assertEquals(len(stored_record.blob_value), 2)
            self.assertEquals(stored_record.blob_value[0], 'local')
            self.assertEquals(stored_record.blob_value[1], 'remote')

            self.assertEquals(
                len(stored_record.cluster_clock.partition_clock), 2)

            # merged record was also returned in response
            self.assertEquals(len(response_record.blob_value), 2)
            self.assertEquals(response_record.blob_value[0], 'local')
            self.assertEquals(response_record.blob_value[1], 'remote')

    def _make_request(self, record):

        server = yield Server.connect_to(
            self.listener.get_address(), self.listener.get_port())

        request = yield server.schedule_request()
        request.get_message().set_type(CommandType.REPLICATE_BLOB)

        repl_req = request.get_message().mutable_replication()
        repl_req.set_table_uuid(UUID(self.table.uuid).to_bytes())
        repl_req.set_partition_uuid(UUID(self.partition.uuid).to_bytes())
        repl_req.set_key(self.key)

        record = record.SerializeToBytes()

        request.get_message().add_data_block_length(len(record))
        request.start_request()
        request.write_interface().queue_write(record)

        yield (yield request.finish_request())

