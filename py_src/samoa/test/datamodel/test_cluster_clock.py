
import random
import unittest

from samoa.core.server_time import ServerTime
from samoa.core.uuid import UUID
from samoa.core.protobuf import ClusterClock, PersistedRecord
from samoa.datamodel.clock_util import ClockUtil, ClockAncestry, MergeStep

class TestClusterClock(unittest.TestCase):

    def test_tick(self):

        def tick_noop(insert, index):
            pass

        A = UUID.from_random()
        clock = ClusterClock()

        def validate(expected_lamport):
            self.assertEquals(len(clock.partition_clock), 1)
            part_clock = clock.partition_clock[0]

            self.assertEquals(A.to_bytes(), part_clock.partition_uuid)
            self.assertEquals(ServerTime.get_time(), part_clock.unix_timestamp)
            self.assertEquals(expected_lamport, part_clock.lamport_tick)

        ClockUtil.tick(clock, A, tick_noop)
        validate(0)
        ClockUtil.tick(clock, A, tick_noop)
        validate(1)
        ClockUtil.tick(clock, A, tick_noop)
        validate(2)

        ServerTime.set_time(ServerTime.get_time() + 1)

        ClockUtil.tick(clock, A, tick_noop)
        validate(0)
        ClockUtil.tick(clock, A, tick_noop)
        validate(1)

        ClockUtil.tick(clock, UUID.from_random(), tick_noop)
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

    def test_compare_and_merge(self):

        def tick_noop(insert, index):
            pass

        def merge_noop(merge_step):
            pass

        A = UUID.from_random()
        B = UUID.from_random()
        C = UUID.from_random()
        IGNORE = UUID.from_random()
        PRUNE = UUID.from_random()

        local_clock = ClusterClock()
        remote_clock = ClusterClock()
        expected_clock = ClusterClock()

        def validate(expected_ancestry):

            self.assertEquals(expected_ancestry,
                ClockUtil.compare(local_clock, remote_clock, 10))

            merged_clock = ClusterClock(local_clock)
            merge_result = ClockUtil.merge(
                merged_clock, remote_clock, 10, merge_noop)

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
            #  use a large horizon to validate IGNORE & PRUNE as well
            self.assertEquals(ClockAncestry.CLOCKS_EQUAL,
                ClockUtil.compare(merged_clock, expected_clock, (1<<31)))

        # divergent remote tick, old enough to be pruned
        ClockUtil.tick(remote_clock, PRUNE, tick_noop)

        ServerTime.set_time(ServerTime.get_time() + \
            ClockUtil.clock_jitter_bound)

        # divergent local tick, old enough to be ignored
        ClockUtil.tick(local_clock, IGNORE, tick_noop)
        ClockUtil.tick(expected_clock, IGNORE, tick_noop)

        ServerTime.set_time(ServerTime.get_time() + 10)

        validate(ClockAncestry.CLOCKS_EQUAL)

        # common tick of A
        ClockUtil.tick(local_clock, A, tick_noop)
        ClockUtil.tick(remote_clock, A, tick_noop)
        ClockUtil.tick(expected_clock, A, tick_noop)
        validate(ClockAncestry.CLOCKS_EQUAL)

        # remote tick of A
        ClockUtil.tick(remote_clock, A, tick_noop)
        ClockUtil.tick(expected_clock, A, tick_noop)
        validate(ClockAncestry.REMOTE_MORE_RECENT)

        # local tick of B
        ClockUtil.tick(local_clock, B, tick_noop)
        ClockUtil.tick(expected_clock, B, tick_noop)
        validate(ClockAncestry.CLOCKS_DIVERGE)

        # local tick of A (catching up)
        ClockUtil.tick(local_clock, A, tick_noop)
        validate(ClockAncestry.LOCAL_MORE_RECENT)

        # remote tick of B (catching up)
        ClockUtil.tick(remote_clock, B, tick_noop)
        validate(ClockAncestry.CLOCKS_EQUAL)

        ServerTime.set_time(ServerTime.get_time() + 1)

        # remote tick of C
        ClockUtil.tick(remote_clock, C, tick_noop)
        ClockUtil.tick(expected_clock, C, tick_noop)
        validate(ClockAncestry.REMOTE_MORE_RECENT)

        # remote tick of B
        ClockUtil.tick(remote_clock, B, tick_noop)
        ClockUtil.tick(expected_clock, B, tick_noop)
        validate(ClockAncestry.REMOTE_MORE_RECENT)

        # local tick of A
        ClockUtil.tick(local_clock, A, tick_noop)
        ClockUtil.tick(expected_clock, A, tick_noop)
        validate(ClockAncestry.CLOCKS_DIVERGE)

    def test_merge_datamodel_update(self):

        def tick_noop(insert, index):
            pass

        local_clock = ClusterClock()
        remote_clock = ClusterClock()

        # partitions to represent each of the possible merge cases
        REMOTE_ONLY = UUID.from_random()
        LOCAL_IGNORE = UUID.from_random()
        REMOTE_NEWER_TS = UUID.from_random()
        LOCAL_NEWER_TS = UUID.from_random()
        REMOTE_NEWER_TICK = UUID.from_random()
        LOCAL_NEWER_TICK = UUID.from_random()
        LOCAL_ONLY = UUID.from_random()
        REMOTE_ONLY = UUID.from_random()

        # prunable remote-only partition
        ClockUtil.tick(remote_clock, REMOTE_ONLY, tick_noop)
        ServerTime.set_time(ServerTime.get_time() + \
            ClockUtil.clock_jitter_bound)

        # ignorable local-only partition
        ClockUtil.tick(local_clock, LOCAL_IGNORE, tick_noop)
        ServerTime.set_time(ServerTime.get_time() + 10)

        # 'old' ticks for timestamp-divergent partitions
        ClockUtil.tick(local_clock,  REMOTE_NEWER_TS, tick_noop)
        ClockUtil.tick(remote_clock, REMOTE_NEWER_TS, tick_noop)
        ClockUtil.tick(local_clock,  LOCAL_NEWER_TS, tick_noop)
        ClockUtil.tick(remote_clock, LOCAL_NEWER_TS, tick_noop)

        ServerTime.set_time(ServerTime.get_time() + 1)

        # 'new' ticks for timestamp-divergent partitions
        ClockUtil.tick(local_clock,  LOCAL_NEWER_TS, tick_noop)
        ClockUtil.tick(remote_clock, REMOTE_NEWER_TS, tick_noop)

        # ticks for lamport-divergent partitions
        ClockUtil.tick(remote_clock, LOCAL_NEWER_TICK, tick_noop)
        ClockUtil.tick(local_clock, LOCAL_NEWER_TICK, tick_noop)
        ClockUtil.tick(local_clock, LOCAL_NEWER_TICK, tick_noop)

        ClockUtil.tick(local_clock, REMOTE_NEWER_TICK, tick_noop)
        ClockUtil.tick(remote_clock, REMOTE_NEWER_TICK, tick_noop)
        ClockUtil.tick(remote_clock, REMOTE_NEWER_TICK, tick_noop)

        # partitions known to only one clock
        ClockUtil.tick(local_clock, LOCAL_ONLY, tick_noop)
        ClockUtil.tick(remote_clock, REMOTE_ONLY, tick_noop)

        expected_merge_steps = {
            REMOTE_ONLY:      MergeStep.RHS_SKIP,
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

        def tick_noop(insert, index):
            pass

        def prune_noop(index):
            pass

        KEEP = UUID.from_random()
        IGNORE = UUID.from_random()
        PRUNE_A = UUID.from_random()
        PRUNE_B = UUID.from_random()

        record = PersistedRecord()
        clock = record.mutable_cluster_clock()

        expect = ClusterClock()

        # two clocks to be pruned
        ClockUtil.tick(clock, PRUNE_A, tick_noop)
        ClockUtil.tick(clock, PRUNE_B, tick_noop)
        ServerTime.set_time(ServerTime.get_time() + \
            ClockUtil.clock_jitter_bound)

        # one clock past ignore_ts, to be kept
        ClockUtil.tick(clock, IGNORE, tick_noop)
        ClockUtil.tick(expect, IGNORE, tick_noop)
        ServerTime.set_time(ServerTime.get_time() + 10)

        # one current clock, to be kept
        ClockUtil.tick(clock, KEEP, tick_noop)
        ClockUtil.tick(expect, KEEP, tick_noop)

        ClockUtil.prune(record, 10, prune_noop)

        # assert pruned clock matches expectation
        self.assertEquals(ClockAncestry.CLOCKS_EQUAL,
            ClockUtil.compare(clock, expect, (1<<31)))

        # jump forward such that all clocks are prunable
        ServerTime.set_time(ServerTime.get_time() + \
            ClockUtil.clock_jitter_bound + 1)

        ClockUtil.prune(record, 1, prune_noop)

        # no clocks remain; cluster_clock has been removed from record
        self.assertFalse(record.has_cluster_clock())

    def _test_prune_record(self):

        A = UUID.from_random()
        B = UUID.from_random()
        C = UUID.from_random()
        D = UUID.from_random()

        precord = PersistedRecord()
        clock = precord.mutable_cluster_clock()

        for i in (A, B, C, D):

            ServerTime.set_time(ServerTime.get_time() \
                + ClockUtil.clock_jitter_bound)

            ClockUtil.tick(clock, i)

        ServerTime.set_time(ServerTime.get_time() + 1)

        self.assertEquals(len(clock.partition_clock), 4)

        # prune, with a consistency horizon of 1 + jitter
        ClockUtil.prune_record(precord, 1 + ClockUtil.clock_jitter_bound)

        # only C & D's tick remains after pruning (B just missed)
        self.assertEquals(len(clock.partition_clock), 2)

        result_ticks = set(UUID.from_bytes(p.partition_uuid) \
            for p in clock.partition_clock)
        self.assertEquals(result_ticks, set([C, D]))

        # prune, with a consistency horizon of 1
        ClockUtil.prune_record(precord, 1)

        # only D remains
        self.assertEquals(len(clock.partition_clock), 1)
        self.assertEquals(D,
            UUID.from_bytes(clock.partition_clock[0].partition_uuid)) 

