
import getty
import unittest

from samoa.core.uuid import UUID
from samoa.core.server_time import ServerTime
from samoa.core.protobuf import PersistedRecord, ClusterClock
from samoa.datamodel.clock_util import ClockUtil, ClockAncestry
from samoa.datamodel.blob import Blob

from samoa.test.module import TestModule


class TestBlob(unittest.TestCase):

    def test_update(self):

        record = PersistedRecord()

        # initial value fixture
        record.add_consistent_blob_value('value')

        self.assertItemsEqual(Blob.value(record), ['value'])

        A = UUID.from_random()
        B = UUID.from_random()

        Blob.update(record, A, 'value-A')
        self.assertItemsEqual(record.blob_value, ['value-A'])
        self.assertItemsEqual(Blob.value(record), ['value-A'])

        Blob.update(record, A, 'value-A-2')
        self.assertItemsEqual(record.blob_value, ['value-A-2'])
        self.assertItemsEqual(Blob.value(record), ['value-A-2'])

        Blob.update(record, B, 'value-B')
        self.assertItemsEqual(record.blob_value, ['value-B', ''])
        self.assertItemsEqual(Blob.value(record), ['value-B'])

    def test_prune(self):

        record = PersistedRecord()

        Blob.update(record, UUID.from_random(), 'replaced')
        Blob.update(record, UUID.from_random(), 'prune_value')

        ServerTime.set_time(ServerTime.get_time() + \
            ClockUtil.clock_jitter_bound + 1)

        # non-empty prunable value => value moved consistent_blob_value
        self.assertFalse(Blob.prune(record, 1))

        self.assertItemsEqual(record.blob_value, [])
        self.assertItemsEqual(record.consistent_blob_value, ['prune_value'])

        Blob.update(record, UUID.from_random(), 'new_value')

        # current clock => prune takes no action
        self.assertFalse(Blob.prune(record, 1))

        self.assertItemsEqual(record.consistent_blob_value, [])
        self.assertItemsEqual(record.blob_value, ['new_value'])

        # put empty string, to mark as deleted
        Blob.update(record, UUID.from_random(), '')

        ServerTime.set_time(ServerTime.get_time() + \
            ClockUtil.clock_jitter_bound + 1)

        # empty prunable value => prune() is True and record is cleared
        self.assertTrue(Blob.prune(record, 1))
        self.assertItemsEqual(record.blob_value, [])
        self.assertItemsEqual(record.consistent_blob_value, [])

        # record is expired => prune() is True
        Blob.update(record, UUID.from_random(), 'foobar')
        record.set_expire_timestamp(ServerTime.get_time() - 1)

        self.assertTrue(Blob.prune(record, 1))

    def test_merge(self):

        # fixtures:

        # one with two pruned values (no clocks)
        # one with one pruned, and one clock < prune_ts
        # one with two clocks < prune_ts
        # one 'new' write of value-A
        # one 'new' write of value-B




        local_record = PersistedRecord()
        remote_record = PersistedRecord()

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

