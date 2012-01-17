
import random
import unittest

from samoa.core.uuid import UUID
from samoa.core.server_time import ServerTime
from samoa.core.protobuf import ClusterClock, PersistedRecord
from samoa.datamodel.clock_util import ClockUtil, ClockAncestry, MergeStep

class TestClusterClock(unittest.TestCase):

    def test_validate(self):

        clock = ClusterClock()

        ClockUtil.tick(clock, UUID.from_random(), self._tick_noop)
        ClockUtil.tick(clock, UUID.from_random(), self._tick_noop)

        # base clock is valid
        self.assertTrue(ClockUtil.validate(clock))

        # bad partition uuid => invalid clock
        tmp = ClusterClock(clock)
        tmp.partition_clock[0].set_partition_uuid(
            tmp.partition_clock[0].partition_uuid[:-1])
        self.assertFalse(ClockUtil.validate(tmp))

        # bad uuid order => invalid clock
        tmp = ClusterClock(clock)
        tmp.partition_clock.SwapElements(0, 1)
        self.assertFalse(ClockUtil.validate(tmp))

    def test_is_consistent(self):

        clock = ClusterClock()

        # has a pruned clock => consistent
        self.assertTrue(ClockUtil.is_consistent(clock, 1))

        # no pruned clock => inconsistent
        clock.set_clock_is_pruned(False)
        self.assertFalse(ClockUtil.is_consistent(clock, 1))

        # no pruned clock & recent tick => inconsistent
        ClockUtil.tick(clock, UUID.from_random(), self._tick_noop)
        self.assertFalse(ClockUtil.is_consistent(clock, 1))

        # tick at least ignore_ts old => consistent
        ServerTime.set_time(ServerTime.get_time() + 1)
        self.assertTrue(ClockUtil.is_consistent(clock, 1))

    def test_tick(self):

        A = UUID.from_random()
        clock = ClusterClock()

        def validate(expected_lamport):
            self.assertEquals(len(clock.partition_clock), 1)
            part_clock = clock.partition_clock[0]

            self.assertEquals(A.to_bytes(), part_clock.partition_uuid)
            self.assertEquals(ServerTime.get_time(), part_clock.unix_timestamp)
            self.assertEquals(expected_lamport, part_clock.lamport_tick)

        ClockUtil.tick(clock, A, self._tick_noop)
        validate(0)
        ClockUtil.tick(clock, A, self._tick_noop)
        validate(1)
        ClockUtil.tick(clock, A, self._tick_noop)
        validate(2)

        ServerTime.set_time(ServerTime.get_time() + 1)

        ClockUtil.tick(clock, A, self._tick_noop)
        validate(0)
        ClockUtil.tick(clock, A, self._tick_noop)
        validate(1)

        ClockUtil.tick(clock, UUID.from_random(), self._tick_noop)
        self.assertEquals(len(clock.partition_clock), 2)

    def test_tick_datamodel_update(self):

        A, B, C = sorted(UUID.from_random() for i in xrange(3))
        clock = ClusterClock()

        ClockUtil.tick(clock, A,
            lambda insert, index: (
                self.assertTrue(insert),
                self.assertEquals(index, 0)))

        ClockUtil.tick(clock, C,
            lambda insert, index: (
                self.assertTrue(insert),
                self.assertEquals(index, 1)))

        ClockUtil.tick(clock, C,
            lambda insert, index: (
                self.assertFalse(insert),
                self.assertEquals(index, 1)))

        ClockUtil.tick(clock, A,
            lambda insert, index: (
                self.assertFalse(insert),
                self.assertEquals(index, 0)))

        ClockUtil.tick(clock, B,
            lambda insert, index: (
                self.assertTrue(insert),
                self.assertEquals(index, 1)))

        ClockUtil.tick(clock, C,
            lambda insert, index: (
                self.assertFalse(insert),
                self.assertEquals(index, 2)))

    def _compare_and_merge_validator_factory(self,
        local_clock, remote_clock, expected_clock, horizon):

        def validate(expected_ancestry):

            self.assertEquals(expected_ancestry,
                ClockUtil.compare(local_clock, remote_clock, horizon))

            merged_clock = ClusterClock(local_clock)
            merge_result = ClockUtil.merge(
                merged_clock, remote_clock, horizon, self._merge_noop)

            if expected_ancestry == ClockAncestry.CLOCKS_EQUAL:
                self.assertFalse(merge_result.local_was_updated)
                self.assertFalse(merge_result.remote_is_stale)

            elif expected_ancestry == ClockAncestry.LOCAL_MORE_RECENT:
                self.assertFalse(merge_result.local_was_updated)
                self.assertTrue(merge_result.remote_is_stale)

            elif expected_ancestry == ClockAncestry.REMOTE_MORE_RECENT:
                self.assertTrue(merge_result.local_was_updated)
                self.assertFalse(merge_result.remote_is_stale)
            else:
                self.assertTrue(merge_result.local_was_updated)
                self.assertTrue(merge_result.remote_is_stale)

            # ensure merged_clock equals expected_clock;
            #  use a large horizon to check for exact equality
            self.assertEquals(ClockAncestry.CLOCKS_EQUAL,
                ClockUtil.compare(merged_clock, expected_clock, (1<<30)))

        return validate

    def test_compare_and_merge(self):

        A = UUID.from_random()
        B = UUID.from_random()
        C = UUID.from_random()
        IGNORE = UUID.from_random()
        PRUNE = UUID.from_random()

        local_clock = ClusterClock()
        remote_clock = ClusterClock()
        expected_clock = ClusterClock()

        validate = self._compare_and_merge_validator_factory(
            local_clock, remote_clock, expected_clock, horizon = 10)

        # divergent remote tick, old enough to be pruned
        ClockUtil.tick(remote_clock, PRUNE, self._tick_noop)

        ServerTime.set_time(ServerTime.get_time() + \
            ClockUtil.clock_jitter_bound)

        # divergent local tick, old enough to be ignored
        ClockUtil.tick(local_clock, IGNORE, self._tick_noop)
        ClockUtil.tick(expected_clock, IGNORE, self._tick_noop)

        ServerTime.set_time(ServerTime.get_time() + 10)
        validate(ClockAncestry.CLOCKS_EQUAL)

        # common tick of A
        ClockUtil.tick(local_clock, A, self._tick_noop)
        ClockUtil.tick(remote_clock, A, self._tick_noop)
        ClockUtil.tick(expected_clock, A, self._tick_noop)
        validate(ClockAncestry.CLOCKS_EQUAL)

        # remote tick of A
        ClockUtil.tick(remote_clock, A, self._tick_noop)
        ClockUtil.tick(expected_clock, A, self._tick_noop)
        validate(ClockAncestry.REMOTE_MORE_RECENT)

        # local tick of B
        ClockUtil.tick(local_clock, B, self._tick_noop)
        ClockUtil.tick(expected_clock, B, self._tick_noop)
        validate(ClockAncestry.CLOCKS_DIVERGE)

        # local tick of A (catching up)
        ClockUtil.tick(local_clock, A, self._tick_noop)
        validate(ClockAncestry.LOCAL_MORE_RECENT)

        # remote tick of B (catching up)
        ClockUtil.tick(remote_clock, B, self._tick_noop)
        validate(ClockAncestry.CLOCKS_EQUAL)

        # tick again, to exercise timestamp ordering
        ServerTime.set_time(ServerTime.get_time() + 1)

        # remote tick of C
        ClockUtil.tick(remote_clock, C, self._tick_noop)
        ClockUtil.tick(expected_clock, C, self._tick_noop)
        validate(ClockAncestry.REMOTE_MORE_RECENT)

        # remote tick of B
        ClockUtil.tick(remote_clock, B, self._tick_noop)
        ClockUtil.tick(expected_clock, B, self._tick_noop)
        validate(ClockAncestry.REMOTE_MORE_RECENT)

        # local tick of A
        ClockUtil.tick(local_clock, A, self._tick_noop)
        ClockUtil.tick(expected_clock, A, self._tick_noop)
        validate(ClockAncestry.CLOCKS_DIVERGE)

    def test_compare_and_merge_consistency(self):

        local_clock = ClusterClock()
        local_clock.set_clock_is_pruned(False)

        remote_clock = ClusterClock()
        remote_clock.set_clock_is_pruned(False)

        expected_clock = ClusterClock()
        expected_clock.set_clock_is_pruned(False)

        validate = self._compare_and_merge_validator_factory(
            local_clock, remote_clock, expected_clock, horizon = 1)

        COMMON = UUID.from_random()
        ClockUtil.tick(local_clock, COMMON, self._tick_noop)
        ClockUtil.tick(remote_clock, COMMON, self._tick_noop)
        ClockUtil.tick(expected_clock, COMMON, self._tick_noop)

        # all clocks are inconsistent & equal
        validate(ClockAncestry.CLOCKS_EQUAL)

        # local clock is consistent, while remote is not
        local_clock.clear_clock_is_pruned()
        expected_clock.clear_clock_is_pruned()

        validate(ClockAncestry.LOCAL_MORE_RECENT)

        # remote clock is consistent, while local is not
        local_clock.set_clock_is_pruned(False)
        remote_clock.clear_clock_is_pruned()

        validate(ClockAncestry.REMOTE_MORE_RECENT)

        # remove local's COMMON tick (as it's age makes local consistent)
        local_clock.Clear()
        local_clock.set_clock_is_pruned(False)

        # a remote tick which would ordinarily be ignored
        PRUNE = UUID.from_random()
        ClockUtil.tick(remote_clock, PRUNE, self._tick_noop)
        ClockUtil.tick(expected_clock, PRUNE, self._tick_noop)

        ServerTime.set_time(ServerTime.get_time() + \
            ClockUtil.clock_jitter_bound + 1)

        validate(ClockAncestry.REMOTE_MORE_RECENT)

        # a divergent local tick
        LOCAL = UUID.from_random()
        ClockUtil.tick(local_clock, LOCAL, self._tick_noop)
        ClockUtil.tick(expected_clock, LOCAL, self._tick_noop)

        validate(ClockAncestry.CLOCKS_DIVERGE)

    def test_merge_datamodel_update(self):

        local_clock = ClusterClock()
        remote_clock = ClusterClock()

        # partitions to represent each of the possible merge cases
        REMOTE_PRUNE = UUID.from_random()
        LOCAL_IGNORE = UUID.from_random()
        REMOTE_NEWER_TS = UUID.from_random()
        LOCAL_NEWER_TS = UUID.from_random()
        REMOTE_NEWER_TICK = UUID.from_random()
        LOCAL_NEWER_TICK = UUID.from_random()
        LOCAL_ONLY = UUID.from_random()
        REMOTE_ONLY = UUID.from_random()

        # prunable remote-only partition
        ClockUtil.tick(remote_clock, REMOTE_PRUNE, self._tick_noop)
        ServerTime.set_time(ServerTime.get_time() + \
            ClockUtil.clock_jitter_bound)

        # ignorable local-only partition
        ClockUtil.tick(local_clock, LOCAL_IGNORE, self._tick_noop)
        ServerTime.set_time(ServerTime.get_time() + 10)

        # 'old' ticks for timestamp-divergent partitions
        ClockUtil.tick(local_clock,  REMOTE_NEWER_TS, self._tick_noop)
        ClockUtil.tick(remote_clock, REMOTE_NEWER_TS, self._tick_noop)
        ClockUtil.tick(local_clock,  LOCAL_NEWER_TS, self._tick_noop)
        ClockUtil.tick(remote_clock, LOCAL_NEWER_TS, self._tick_noop)

        ServerTime.set_time(ServerTime.get_time() + 1)

        # 'new' ticks for timestamp-divergent partitions
        ClockUtil.tick(local_clock,  LOCAL_NEWER_TS, self._tick_noop)
        ClockUtil.tick(remote_clock, REMOTE_NEWER_TS, self._tick_noop)

        # ticks for lamport-divergent partitions
        ClockUtil.tick(remote_clock, LOCAL_NEWER_TICK, self._tick_noop)
        ClockUtil.tick(local_clock, LOCAL_NEWER_TICK, self._tick_noop)
        ClockUtil.tick(local_clock, LOCAL_NEWER_TICK, self._tick_noop)

        ClockUtil.tick(local_clock, REMOTE_NEWER_TICK, self._tick_noop)
        ClockUtil.tick(remote_clock, REMOTE_NEWER_TICK, self._tick_noop)
        ClockUtil.tick(remote_clock, REMOTE_NEWER_TICK, self._tick_noop)

        # partitions known to only one clock
        ClockUtil.tick(local_clock, LOCAL_ONLY, self._tick_noop)
        ClockUtil.tick(remote_clock, REMOTE_ONLY, self._tick_noop)

        expected_merge_steps = {
            REMOTE_PRUNE:      MergeStep.RHS_SKIP,
            LOCAL_IGNORE:      MergeStep.LHS_ONLY,
            REMOTE_NEWER_TS:   MergeStep.RHS_NEWER,
            LOCAL_NEWER_TS:    MergeStep.LHS_NEWER,
            REMOTE_NEWER_TICK: MergeStep.RHS_NEWER,
            LOCAL_NEWER_TICK:  MergeStep.LHS_NEWER,
            LOCAL_ONLY:        MergeStep.LHS_ONLY,
            REMOTE_ONLY:       MergeStep.RHS_ONLY}

        # given the clock fixtures, validate that we see the
        #  expected ordering of MergeStep values
        expected_merge_steps = [
            v for k, v in sorted(expected_merge_steps.items())]

        def merge_update(merge_step):
            self.assertEquals(merge_step, expected_merge_steps.pop(0))

        ClockUtil.merge(local_clock, remote_clock, 10, merge_update)

    def test_prune(self):

        KEEP = UUID.from_random()
        IGNORE = UUID.from_random()
        PRUNE_A = UUID.from_random()
        PRUNE_B = UUID.from_random()

        record = PersistedRecord()
        clock = record.mutable_cluster_clock()
        clock.set_clock_is_pruned(False)

        expect = ClusterClock()

        # two clocks to be pruned
        ClockUtil.tick(clock, PRUNE_A, self._tick_noop)
        ClockUtil.tick(clock, PRUNE_B, self._tick_noop)
        ServerTime.set_time(ServerTime.get_time() + \
            ClockUtil.clock_jitter_bound)

        # one clock past ignore_ts, to be kept
        ClockUtil.tick(clock, IGNORE, self._tick_noop)
        ClockUtil.tick(expect, IGNORE, self._tick_noop)
        ServerTime.set_time(ServerTime.get_time() + 10)

        # one current clock, to be kept
        ClockUtil.tick(clock, KEEP, self._tick_noop)
        ClockUtil.tick(expect, KEEP, self._tick_noop)

        ClockUtil.prune(record, 10, self._prune_noop)

        # assert pruned clock matches expectation
        self.assertEquals(ClockAncestry.CLOCKS_EQUAL,
            ClockUtil.compare(clock, expect, (1<<30)))

        # jump forward such that all clocks are prunable
        ServerTime.set_time(ServerTime.get_time() + \
            ClockUtil.clock_jitter_bound + 1)

        ClockUtil.prune(record, 1, self._prune_noop)

        # no clocks remain; cluster_clock has been removed from record
        self.assertFalse(record.has_cluster_clock())

    def test_prune_datamodel_update(self):

        PRUNE_A, B, PRUNE_C, D, PRUNE_E = \
            sorted(UUID.from_random() for i in xrange(5))

        record = PersistedRecord()
        clock = record.mutable_cluster_clock()

        # prunable ticks
        ClockUtil.tick(clock, PRUNE_A, self._tick_noop)
        ClockUtil.tick(clock, PRUNE_C, self._tick_noop)
        ClockUtil.tick(clock, PRUNE_E, self._tick_noop)

        ServerTime.set_time(ServerTime.get_time() + \
            ClockUtil.clock_jitter_bound + 1)

        # other ticks
        ClockUtil.tick(clock, B, self._tick_noop)
        ClockUtil.tick(clock, D, self._tick_noop)

        expected_prune_indices = [
            0, # prune of A
            1, # prune of C, after A's removal
            2, # prune of E, after A & C's removal
        ]

        def prune_update(index):
            self.assertEquals(index, expected_prune_indices.pop(0))

        ClockUtil.prune(record, 1, prune_update)

    def _tick_noop(self, insert, index):
        pass 

    def _merge_noop(self, step):
        pass

    def _prune_noop(self, index):
        pass
