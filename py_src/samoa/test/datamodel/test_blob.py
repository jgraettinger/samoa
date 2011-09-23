
import getty
import unittest

from samoa.core.uuid import UUID
from samoa.core.protobuf import PersistedRecord, ClusterClock
from samoa.datamodel.clock_util import ClockUtil, ClockAncestry
from samoa.datamodel.blob import Blob

from samoa.test.module import TestModule


class TestBlob(unittest.TestCase):

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
        local_record.add_blob_value('local-value')

        if local_clock:
            local_record.mutable_cluster_clock().CopyFrom(local_clock)

        # set the remote record fixture
        remote_record = PersistedRecord()
        remote_record.add_blob_value('remote-value')

        if remote_clock:
            remote_record.mutable_cluster_clock().CopyFrom(remote_clock)

        merged_record = PersistedRecord(local_record)
        merge_result = Blob.consistent_merge(merged_record, remote_record, 1)

        if expect == 'no_change':

            self.assertFalse(merge_result.local_was_updated)
            self.assertFalse(merge_result.remote_is_stale)

            self.assertEquals(merged_record.SerializeToBytes(),
                local_record.SerializeToBytes())

        elif expect == 'local':

            self.assertFalse(merge_result.local_was_updated)
            self.assertTrue(merge_result.remote_is_stale)

            self.assertEquals(merged_record.SerializeToBytes(),
                local_record.SerializeToBytes())

        elif expect == 'remote':

            self.assertTrue(merge_result.local_was_updated)
            self.assertFalse(merge_result.remote_is_stale)

            self.assertEquals(merged_record.SerializeToBytes(),
                remote_record.SerializeToBytes())

        elif expect == 'both':

            self.assertTrue(merge_result.local_was_updated)
            self.assertTrue(merge_result.remote_is_stale)

            self.assertEquals(len(merged_record.blob_value), 2)
            self.assertEquals(
                set(['local-value', 'remote-value']),
                set([merged_record.blob_value[0], merged_record.blob_value[1]]))

