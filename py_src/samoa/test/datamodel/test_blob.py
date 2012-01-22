
import getty
import unittest

from samoa.core.uuid import UUID
from samoa.core.server_time import ServerTime
from samoa.core.protobuf import PersistedRecord, ClusterClock
from samoa.datamodel.clock_util import ClockUtil, ClockAncestry
from samoa.datamodel.blob import Blob

HORIZON = 3600

class TestBlob(unittest.TestCase):

    def test_value(self):

        cur_ts = ServerTime.get_time()

        record = self._build_blob([
                (ClockUtil.generate_author_id(), 1, cur_ts, 'value-1'),
                (ClockUtil.generate_author_id(), 1, cur_ts, 'value-2'),
                (ClockUtil.generate_author_id(), 1, cur_ts, ''),
            ], ['old-value-1', 'old-value-2'])

        self.assertItemsEqual(Blob.value(record),
            ['value-1', 'value-2', 'old-value-1', 'old-value-2'])

    def test_update(self):

        auth_A = ClockUtil.generate_author_id()
        auth_B = ClockUtil.generate_author_id()

        cur_ts = ServerTime.get_time()

        # update of empty record
        record = self._build_blob([])
        Blob.update(record, auth_A, 'value')
        self.assertItemsEqual(Blob.value(record), ['value'])

        # update of record with one current value
        record = self._build_blob([(auth_A, 1, cur_ts, 'old-value')])
        Blob.update(record, auth_B, 'new-value')
        self.assertItemsEqual(Blob.value(record), ['new-value'])

        # update of record with one consistent value
        record = self._build_blob([], consistent_values = ['old-value'])
        Blob.update(record, auth_A, 'new-value')
        self.assertItemsEqual(Blob.value(record), ['new-value'])
        self.assertFalse(len(record.blob_consistent_value))

        # update of record with multiple current & consistent values
        record = self._build_blob([
                (auth_A, 1, cur_ts, 'new-value-1'),
                (auth_B, 1, cur_ts, 'new-value-2'),
            ], consistent_values = ['old-value-1', 'old-value-2'])
        Blob.update(record, auth_B, 'new-value')
        self.assertItemsEqual(Blob.value(record), ['new-value'])
        self.assertFalse(len(record.blob_consistent_value))

    def test_prune(self):

        cur_ts = ServerTime.get_time()
        ignore_ts = ServerTime.get_time() - HORIZON
        prune_ts  = ignore_ts - ClockUtil.clock_jitter_bound

        # prunable values are shifted to blob_consistent_value
        record = self._build_blob([
                (ClockUtil.generate_author_id(), 1, cur_ts, 'kept-value-1'),
                (ClockUtil.generate_author_id(), 1, ignore_ts, 'kept-value-2'),
                (ClockUtil.generate_author_id(), 1, prune_ts, 'prune-value-1'),
                (ClockUtil.generate_author_id(), 1, prune_ts, 'prune-value-2'),
                (ClockUtil.generate_author_id(), 1, prune_ts, '')])

        self.assertFalse(Blob.prune(record, HORIZON))
        self.assertItemsEqual(record.blob_value,
            ['kept-value-1', 'kept-value-2'])
        self.assertItemsEqual(record.blob_consistent_value,
            ['prune-value-1', 'prune-value-2'])

        # all authors prunable, setting a consistent value
        record = self._build_blob([
                (ClockUtil.generate_author_id(), 1, prune_ts, 'prune-value-1'),
                (ClockUtil.generate_author_id(), 1, prune_ts, '')])

        self.assertFalse(Blob.prune(record, HORIZON))
        self.assertItemsEqual(record.blob_value, [])
        self.assertItemsEqual(record.blob_consistent_value, ['prune-value-1'])

        # all authors prunable with empty value & legacy consistent value 
        record = self._build_blob([
                (ClockUtil.generate_author_id(), 1, prune_ts, ''),
            ], consistent_values = ['old-value-1'])

        self.assertFalse(Blob.prune(record, HORIZON))
        self.assertItemsEqual(record.blob_value, [])
        self.assertItemsEqual(record.blob_consistent_value, ['old-value-1'])

        # all authors prunable, & no consistent value
        record = self._build_blob([
                (ClockUtil.generate_author_id(), 1, prune_ts, '')])
        self.assertTrue(Blob.prune(record, HORIZON))

        # current value, but expiry has passed
        record = self._build_blob([
                (ClockUtil.generate_author_id(), 1, cur_ts, 'kept-value-1')])
        record.set_expire_timestamp(prune_ts)
        self.assertTrue(Blob.prune(record, HORIZON))

    def test_merge_scenarios(self):

        auth_A = ClockUtil.generate_author_id()
        auth_B = ClockUtil.generate_author_id()
        auth_C = ClockUtil.generate_author_id()

        cur_ts = ServerTime.get_time()
        ignore_ts = ServerTime.get_time() - HORIZON
        prune_ts  = ignore_ts - ClockUtil.clock_jitter_bound

        # local & remote equal
        local = self._build_blob([
                (auth_A, 1, cur_ts, 'value-1'),
                (auth_B, 1, cur_ts, 'value-2')])

        result = Blob.merge(local, self._build_blob([
                (auth_A, 1, cur_ts, 'value-1'),
                (auth_B, 1, cur_ts, 'value-2')]),
            HORIZON)

        self.assertItemsEqual(Blob.value(local), ['value-1', 'value-2'])
        self.assertFalse(result.local_was_updated)
        self.assertFalse(result.remote_is_stale)

        # remote more-recent
        local = self._build_blob([
                (auth_A, 1, cur_ts, 'value-1'),
                (auth_B, 1, cur_ts, 'value-2')])

        result = Blob.merge(local, self._build_blob([
                (auth_A, 1, cur_ts, ''),
                (auth_B, 2, cur_ts, 'new-value')]),
            HORIZON)

        self.assertItemsEqual(Blob.value(local), ['new-value'])
        self.assertTrue(result.local_was_updated)
        self.assertFalse(result.remote_is_stale)

        # remote more-recent (deletion)
        local = self._build_blob([
                (auth_A, 1, cur_ts, 'value-1'),
                (auth_B, 1, cur_ts, 'value-2')])

        result = Blob.merge(local, self._build_blob([
                (auth_A, 1, cur_ts, ''),
                (auth_B, 2, cur_ts, '')]),
            HORIZON)

        self.assertItemsEqual(Blob.value(local), [])
        self.assertTrue(result.local_was_updated)
        self.assertFalse(result.remote_is_stale)

        # local more-recent
        local = self._build_blob([
                (auth_A, 1, cur_ts, ''),
                (auth_B, 2, cur_ts, 'new-value')])

        result = Blob.merge(local, self._build_blob([
                (auth_A, 1, cur_ts, 'value-1'),
                (auth_B, 1, cur_ts, 'value-2')]),
            HORIZON)

        self.assertItemsEqual(Blob.value(local), ['new-value'])
        self.assertFalse(result.local_was_updated)
        self.assertTrue(result.remote_is_stale)

        # local & remote have divergent current read-repair
        local = self._build_blob([
                (auth_A, 1, cur_ts, ''),
                (auth_B, 2, cur_ts, 'new-value-1'),
                (auth_C, 1, cur_ts, 'old-value')])

        result = Blob.merge(local, self._build_blob([
                (auth_A, 2, cur_ts, 'new-value-2'),
                (auth_B, 1, cur_ts, ''),
                (auth_C, 1, cur_ts, '')]),
            HORIZON)

        self.assertItemsEqual(Blob.value(local),
            ['new-value-1', 'new-value-2'])
        self.assertTrue(result.local_was_updated)
        self.assertTrue(result.remote_is_stale)

        # consistent local; remote with current & pruneable
        local = self._build_blob([], ['old-value'])

        result = Blob.merge(local, self._build_blob([
                (auth_A, 1, prune_ts, 'old-value'),
                (auth_B, 1, cur_ts, 'new-value')]),
            HORIZON)

        self.assertItemsEqual(Blob.value(local), ['old-value', 'new-value'])
        self.assertTrue(result.local_was_updated)
        self.assertFalse(result.remote_is_stale)

        # consistent local; remote with current & consistent
        local = self._build_blob([], ['old-value'])

        result = Blob.merge(local, self._build_blob([
                (auth_A, 1, cur_ts, 'new-value')],
                ['old-value']),
            HORIZON)

        self.assertItemsEqual(Blob.value(local), ['old-value', 'new-value'])
        self.assertTrue(result.local_was_updated)
        self.assertFalse(result.remote_is_stale)

        # pruned local; remote with empty pruned & current
        local = self._build_blob([], ['old-value'])

        result = Blob.merge(local, self._build_blob([
                (auth_A, 1, cur_ts, 'new-value')],
                []),
            HORIZON)

        self.assertItemsEqual(Blob.value(local), ['new-value'])
        self.assertTrue(result.local_was_updated)
        self.assertFalse(result.remote_is_stale)

        # ignorable local; remote with pruned & current
        local = self._build_blob([
                (auth_A, 1, ignore_ts, 'old-value')])

        result = Blob.merge(local, self._build_blob([
                (auth_B, 1, cur_ts, 'new-value')],
                ['old-value']),
            HORIZON)

        self.assertItemsEqual(Blob.value(local), ['old-value', 'new-value'])
        self.assertTrue(result.local_was_updated)
        self.assertFalse(result.remote_is_stale)

        # ignorable local; remote with empty pruned & current
        local = self._build_blob([
                (auth_A, 1, ignore_ts, 'old-value')])

        result = Blob.merge(local, self._build_blob([
                (auth_B, 1, cur_ts, 'new-value')],
                []),
            HORIZON)

        self.assertItemsEqual(Blob.value(local), ['new-value'])
        self.assertTrue(result.local_was_updated)
        self.assertFalse(result.remote_is_stale)

        # current new local; remote with pruned
        local = self._build_blob([
                (auth_A, 1, cur_ts, 'new-value')])

        result = Blob.merge(local,
            self._build_blob([], ['old-value']),
            HORIZON)

        self.assertItemsEqual(Blob.value(local), ['old-value', 'new-value'])
        self.assertTrue(result.local_was_updated)
        self.assertTrue(result.remote_is_stale)

        # legacy local, with one repaired & one divergent author,
        #  new remote with newer & divergent author
        local = self._build_blob([
                (auth_A, 1, cur_ts, 'replaced-value'),
                (auth_B, 1, cur_ts, 'new-value-1')],
                ['old-value'])

        resut = Blob.merge(local, self._build_blob([
                (auth_A, 2, cur_ts, ''),
                (auth_C, 1, cur_ts, 'new-value-2')]),
            HORIZON)

        self.assertItemsEqual(Blob.value(local),
            ['old-value', 'new-value-1', 'new-value-2'])
        self.assertTrue(result.local_was_updated)
        self.assertTrue(result.remote_is_stale)

        # legacy remote, with one repaired & one divergent author,
        #  new local with newer & divergent author
        local = self._build_blob([
                (auth_A, 2, cur_ts, ''),
                (auth_C, 1, cur_ts, 'new-value-2')])

        resut = Blob.merge(local, self._build_blob([
                (auth_A, 1, cur_ts, 'replaced-value'),
                (auth_B, 1, cur_ts, 'new-value-1')],
                ['old-value']),
            HORIZON)

        self.assertItemsEqual(Blob.value(local),
            ['old-value', 'new-value-1', 'new-value-2'])
        self.assertTrue(result.local_was_updated)
        self.assertTrue(result.remote_is_stale)

    def _build_blob(self, ticks, consistent_values = None):

        record = PersistedRecord()
        cluster_clock = record.mutable_cluster_clock()

        if consistent_values is None:
            cluster_clock.set_clock_is_pruned(False)

        author_value = {}

        def tick_noop(insert, index):
            pass 

        for author_id, tick_count, tick_ts, value in ticks:
            restore_ts = ServerTime.get_time()
            ServerTime.set_time(tick_ts)

            for i in xrange(tick_count):
                ClockUtil.tick(cluster_clock, author_id, tick_noop)

            author_value[author_id] = value
            ServerTime.set_time(restore_ts)

        for author_clock in cluster_clock.author_clock:
            record.add_blob_value(author_value[author_clock.author_id])

        for consistent_value in (consistent_values or ()):
            record.add_blob_consistent_value(consistent_value)

        return record

