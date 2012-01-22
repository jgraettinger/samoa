
import getty
import unittest

from samoa.core.uuid import UUID
from samoa.core.server_time import ServerTime
from samoa.core.protobuf import PersistedRecord, ClusterClock
from samoa.datamodel.clock_util import ClockUtil, ClockAncestry
from samoa.datamodel.counter import Counter

HORIZON = 3600

class TestCounter(unittest.TestCase):

    def test_value(self):

        cur_ts = ServerTime.get_time()

        record = self._build_counter([
                (ClockUtil.generate_author_id(), 1, cur_ts, 1),
                (ClockUtil.generate_author_id(), 1, cur_ts, 2),
                (ClockUtil.generate_author_id(), 1, cur_ts, 3),
            ], 4)

        self.assertEquals(Counter.value(record), 10)

    def test_update(self):

        auth_A = ClockUtil.generate_author_id()
        auth_B = ClockUtil.generate_author_id()

        cur_ts = ServerTime.get_time()

        # update of empty record
        record = self._build_counter([])
        Counter.update(record, auth_A, 5)
        self.assertEquals(Counter.value(record), 5)

        # update of record with one current value
        record = self._build_counter([(auth_A, 1, cur_ts, 5)])
        Counter.update(record, auth_B, 6)
        self.assertEquals(Counter.value(record), 11)

        # update of record with one consistent value
        record = self._build_counter([], consistent_value = 11)
        Counter.update(record, auth_A, 12)
        self.assertEquals(Counter.value(record), 23)

        # update of record with multiple current & consistent values
        record = self._build_counter([
                (auth_A, 1, cur_ts, 5),
                (auth_B, 1, cur_ts, 6),
            ], consistent_value = 7)

        Counter.update(record, auth_B, 8)
        self.assertEquals(Counter.value(record), 26)

    def test_prune(self):

        cur_ts = ServerTime.get_time()
        ignore_ts = ServerTime.get_time() - HORIZON
        prune_ts  = ignore_ts - ClockUtil.clock_jitter_bound

        # prunable values are folded into counter_consistent_value
        record = self._build_counter([
                (ClockUtil.generate_author_id(), 1, cur_ts,    1),
                (ClockUtil.generate_author_id(), 1, ignore_ts, 2),
                (ClockUtil.generate_author_id(), 1, prune_ts,  3),
                (ClockUtil.generate_author_id(), 1, prune_ts,  4),
                (ClockUtil.generate_author_id(), 1, prune_ts,  5)])

        self.assertFalse(Counter.prune(record, HORIZON))
        self.assertItemsEqual(record.counter_value, [1, 2])
        self.assertEquals(record.counter_consistent_value, 12)

        # all authors prunable, setting a consistent value
        record = self._build_counter([
                (ClockUtil.generate_author_id(), 1, prune_ts, 1),
                (ClockUtil.generate_author_id(), 1, prune_ts, 2)])

        self.assertFalse(Counter.prune(record, HORIZON))
        self.assertItemsEqual(record.counter_value, [])
        self.assertEquals(record.counter_consistent_value, 3)

        # current value, but expiry has passed
        record = self._build_counter([
                (ClockUtil.generate_author_id(), 1, cur_ts, 5)])
        record.set_expire_timestamp(prune_ts)
        self.assertTrue(Counter.prune(record, HORIZON))


    def test_merge_scenarios(self):

        auth_A = ClockUtil.generate_author_id()
        auth_B = ClockUtil.generate_author_id()
        auth_C = ClockUtil.generate_author_id()

        cur_ts = ServerTime.get_time()
        ignore_ts = ServerTime.get_time() - HORIZON
        prune_ts  = ignore_ts - ClockUtil.clock_jitter_bound

        # local & remote equal
        local = self._build_counter([
                (auth_A, 1, cur_ts, 3),
                (auth_B, 1, cur_ts, 6)])

        result = Counter.merge(local, self._build_counter([
                (auth_A, 1, cur_ts, 3),
                (auth_B, 1, cur_ts, 6)]),
            HORIZON)

        self.assertEquals(Counter.value(local), 9)
        self.assertFalse(result.local_was_updated)
        self.assertFalse(result.remote_is_stale)

        # more-recent remote
        local = self._build_counter([
                (auth_A, 1, cur_ts, 3),
                (auth_B, 1, cur_ts, 6)])

        result = Counter.merge(local, self._build_counter([
                (auth_A, 1, cur_ts, 3),
                (auth_B, 2, cur_ts, 7)]),
            HORIZON)

        self.assertEquals(Counter.value(local), 10)
        self.assertTrue(result.local_was_updated)
        self.assertFalse(result.remote_is_stale)

        # more-recent local
        local = self._build_counter([
                (auth_A, 1, cur_ts, 3),
                (auth_B, 2, cur_ts, 7)])

        result = Counter.merge(local, self._build_counter([
                (auth_A, 1, cur_ts, 3),
                (auth_B, 1, cur_ts, 6)]),
            HORIZON)

        self.assertEquals(Counter.value(local), 10)
        self.assertFalse(result.local_was_updated)
        self.assertTrue(result.remote_is_stale)

        # local & remote diverge
        local = self._build_counter([
                (auth_A, 1, cur_ts, 3),
                (auth_B, 2, cur_ts, 7)])

        result = Counter.merge(local, self._build_counter([
                (auth_A, 2, cur_ts, 1),
                (auth_B, 1, cur_ts, 6)]),
            HORIZON)

        self.assertEquals(Counter.value(local), 8)
        self.assertTrue(result.local_was_updated)
        self.assertTrue(result.remote_is_stale)

        # consistent local; remote with current & pruneable
        local = self._build_counter([], 5)

        result = Counter.merge(local, self._build_counter([
                (auth_A, 1, prune_ts, 5),
                (auth_B, 1, cur_ts, 2)]),
            HORIZON)

        self.assertEquals(Counter.value(local), 7)
        self.assertTrue(result.local_was_updated)
        self.assertFalse(result.remote_is_stale)

        # consistent local; remote with current & consistent
        local = self._build_counter([], 5)

        result = Counter.merge(local, self._build_counter([
                (auth_A, 1, cur_ts, 9)], 5),
            HORIZON)

        self.assertEquals(Counter.value(local), 14)
        self.assertTrue(result.local_was_updated)
        self.assertFalse(result.remote_is_stale)

        # ignorable local; remote with current & consistent
        local = self._build_counter([
                (auth_A, 1, ignore_ts, 5)])

        result = Counter.merge(local, self._build_counter([
                (auth_B, 1, cur_ts, 6)], 5),
            HORIZON)

        self.assertEquals(Counter.value(local), 11)
        self.assertTrue(result.local_was_updated)
        self.assertFalse(result.remote_is_stale)

        # local with ignorable & consistent; remote with
        #  pruneable, current, & consistent
        local = self._build_counter([
                (auth_A, 1, ignore_ts, 4)], 5)

        result = Counter.merge(local, self._build_counter([
                (auth_B, 1, prune_ts, 7),
                (auth_C, 1, cur_ts,   9)], 2),
            HORIZON)

        self.assertEquals(Counter.value(local), 18)
        self.assertTrue(result.local_was_updated)
        self.assertFalse(result.remote_is_stale)

        # current new local; remote with consistent & current
        local = self._build_counter([
                (auth_A, 1, cur_ts, 7)])

        result = Counter.merge(local, self._build_counter([
                (auth_B, 1, cur_ts, 3)], 9),
            HORIZON)

        self.assertEquals(Counter.value(local), 19)
        self.assertTrue(result.local_was_updated)
        self.assertTrue(result.remote_is_stale)

        # current new local; remote with pruneable & current
        local = self._build_counter([
                (auth_A, 1, cur_ts, 7)])

        result = Counter.merge(local, self._build_counter([
                (auth_B, 1, cur_ts, 3),
                (auth_C, 1, prune_ts, 9)]),
            HORIZON)

        self.assertEquals(Counter.value(local), 19)
        self.assertTrue(result.local_was_updated)
        self.assertTrue(result.remote_is_stale)

        # divergent updates to local & remote; local is consistent
        local = self._build_counter([
                (auth_A, 1, cur_ts, 4),
                (auth_B, 1, cur_ts, 3)], 7)

        resut = Counter.merge(local, self._build_counter([
                (auth_A, 2, cur_ts, 5),
                (auth_C, 1, cur_ts, 9)]),
            HORIZON)

        self.assertEquals(Counter.value(local), 24)
        self.assertTrue(result.local_was_updated)
        self.assertTrue(result.remote_is_stale)

        # divergent updates to local & remote; remote is consistent
        local = self._build_counter([
                (auth_A, 2, cur_ts, 5),
                (auth_B, 1, cur_ts, 3)])

        resut = Counter.merge(local, self._build_counter([
                (auth_A, 1, cur_ts, 4),
                (auth_C, 1, cur_ts, 9)], 7),
            HORIZON)

        self.assertEquals(Counter.value(local), 24)
        self.assertTrue(result.local_was_updated)
        self.assertTrue(result.remote_is_stale)

    def _build_counter(self, ticks, consistent_value = None):

        record = PersistedRecord()
        cluster_clock = record.mutable_cluster_clock()

        if consistent_value is None:
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
            record.add_counter_value(author_value[author_clock.author_id])

        if consistent_value:
            record.set_counter_consistent_value(consistent_value)

        return record

