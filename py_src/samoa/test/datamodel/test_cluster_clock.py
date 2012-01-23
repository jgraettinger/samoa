
import random
import unittest

from samoa.core.uuid import UUID
from samoa.core.server_time import ServerTime
from samoa.core.protobuf import ClusterClock, PersistedRecord
from samoa.datamodel.clock_util import ClockUtil, ClockAncestry, MergeStep

HORIZON = 3600

class TestClusterClock(unittest.TestCase):

    def test_validate(self):

        clock = ClusterClock()

        ClockUtil.tick(clock, ClockUtil.generate_author_id(), self._tick_noop)
        ClockUtil.tick(clock, ClockUtil.generate_author_id(), self._tick_noop)

        # base clock is valid
        self.assertTrue(ClockUtil.validate(clock))

        # bad author_id order => invalid clock
        tmp = ClusterClock(clock)
        tmp.author_clock.SwapElements(0, 1)
        self.assertFalse(ClockUtil.validate(tmp))

    def test_tick(self):

        author_id = ClockUtil.generate_author_id()
        clock = ClusterClock()

        def validate(expected_lamport):
            self.assertEquals(len(clock.author_clock), 1)
            author_clock = clock.author_clock[0]

            self.assertEquals(author_id, author_clock.author_id)
            self.assertEquals(ServerTime.get_time(),
                author_clock.unix_timestamp)
            self.assertEquals(expected_lamport, author_clock.lamport_tick)

        ClockUtil.tick(clock, author_id, self._tick_noop)
        validate(0)
        ClockUtil.tick(clock, author_id, self._tick_noop)
        validate(1)
        ClockUtil.tick(clock, author_id, self._tick_noop)
        validate(2)

        ServerTime.set_time(ServerTime.get_time() + 1)

        ClockUtil.tick(clock, author_id, self._tick_noop)
        validate(0)
        ClockUtil.tick(clock, author_id, self._tick_noop)
        validate(1)

        ClockUtil.tick(clock, ClockUtil.generate_author_id(), self._tick_noop)
        self.assertEquals(len(clock.author_clock), 2)

    def test_tick_datamodel_update(self):

        author_A, author_B, author_C = sorted(
            ClockUtil.generate_author_id() for i in xrange(3))
        clock = ClusterClock()

        ClockUtil.tick(clock, author_A,
            lambda insert, index: (
                self.assertTrue(insert),
                self.assertEquals(index, 0)))

        ClockUtil.tick(clock, author_C,
            lambda insert, index: (
                self.assertTrue(insert),
                self.assertEquals(index, 1)))

        ClockUtil.tick(clock, author_C,
            lambda insert, index: (
                self.assertFalse(insert),
                self.assertEquals(index, 1)))

        ClockUtil.tick(clock, author_A,
            lambda insert, index: (
                self.assertFalse(insert),
                self.assertEquals(index, 0)))

        ClockUtil.tick(clock, author_B,
            lambda insert, index: (
                self.assertTrue(insert),
                self.assertEquals(index, 1)))

        ClockUtil.tick(clock, author_C,
            lambda insert, index: (
                self.assertFalse(insert),
                self.assertEquals(index, 2)))

    def _validate_merge(self, local, remote, expect, ancestry):

        local_clock  = self._build_clock(*local)
        remote_clock = self._build_clock(*remote)
        expect_clock = self._build_clock(*expect)

        self.assertEquals(ancestry,
            ClockUtil.compare(local_clock, remote_clock, HORIZON))

        merged_clock = ClusterClock(local_clock)
        merge_result = ClockUtil.merge(merged_clock, remote_clock,
            HORIZON, self._merge_noop)

        if ancestry == ClockAncestry.CLOCKS_EQUAL:
            self.assertFalse(merge_result.local_was_updated)
            self.assertFalse(merge_result.remote_is_stale)

        elif ancestry == ClockAncestry.LOCAL_MORE_RECENT:
            self.assertFalse(merge_result.local_was_updated)
            self.assertTrue(merge_result.remote_is_stale)

        elif ancestry == ClockAncestry.REMOTE_MORE_RECENT:
            self.assertTrue(merge_result.local_was_updated)
            self.assertFalse(merge_result.remote_is_stale)
        else:
            self.assertTrue(merge_result.local_was_updated)
            self.assertTrue(merge_result.remote_is_stale)

        #print "local"
        #print local_clock.SerializeToText()
        #print "remote"
        #print remote_clock.SerializeToText()
        #print "merged"
        #print merged_clock.SerializeToText()
        #print "expect"
        #print expect_clock.SerializeToText()

        # ensure merged_clock equals expected_clock;
        #  use a large horizon to check for exact equality
        self.assertEquals(ClockAncestry.CLOCKS_EQUAL,
            ClockUtil.compare(merged_clock, expect_clock))

    def _build_clock(self, ticks, is_pruned = False):
        clock = ClusterClock()

        if not is_pruned:
            clock.set_clock_is_pruned(False)

        for author_id, tick_count, tick_ts in ticks:
            restore_ts = ServerTime.get_time()
            ServerTime.set_time(tick_ts)

            for i in xrange(tick_count):
                ClockUtil.tick(clock, author_id, self._tick_noop)

            ServerTime.set_time(restore_ts)

        return clock

    def test_merge_scenario_fixtures(self):

        auth_A = ClockUtil.generate_author_id()
        auth_B = ClockUtil.generate_author_id()
        auth_C = ClockUtil.generate_author_id()

        cur_ts = ServerTime.get_time()
        ignore_ts = ServerTime.get_time() - HORIZON
        prune_ts = ignore_ts - ClockUtil.clock_jitter_bound

        # equal author clocks
        self._validate_merge(
            local  = ([(auth_A, 1, cur_ts)], False), 
            remote = ([(auth_A, 1, cur_ts)], False),
            expect = ([(auth_A, 1, cur_ts)], False),
            ancestry = ClockAncestry.CLOCKS_EQUAL)

        # local timestamp newer
        self._validate_merge(
            local  = ([(auth_A, 1, cur_ts)], False), 
            remote = ([(auth_A, 1, cur_ts - 1)], False),
            expect = ([(auth_A, 1, cur_ts)], False),
            ancestry = ClockAncestry.LOCAL_MORE_RECENT)

        # remote timestamp newer
        self._validate_merge(
            local  = ([(auth_A, 1, cur_ts - 1)], False), 
            remote = ([(auth_A, 1, cur_ts)], False),
            expect = ([(auth_A, 1, cur_ts)], False),
            ancestry = ClockAncestry.REMOTE_MORE_RECENT)

        # local lamport newer
        self._validate_merge(
            local  = ([(auth_A, 2, cur_ts)], False), 
            remote = ([(auth_A, 1, cur_ts)], False),
            expect = ([(auth_A, 2, cur_ts)], False),
            ancestry = ClockAncestry.LOCAL_MORE_RECENT)

        # remote lamport newer
        self._validate_merge(
            local  = ([(auth_A, 1, cur_ts)], False), 
            remote = ([(auth_A, 2, cur_ts)], False),
            expect = ([(auth_A, 2, cur_ts)], False),
            ancestry = ClockAncestry.REMOTE_MORE_RECENT)

        # remote has new current author clock
        self._validate_merge(
            local  = ([(auth_A, 1, cur_ts)], False), 
            remote = ([(auth_A, 1, cur_ts), (auth_B, 1, cur_ts)], False),
            expect = ([(auth_A, 1, cur_ts), (auth_B, 1, cur_ts)], False),
            ancestry = ClockAncestry.REMOTE_MORE_RECENT)

        # local has new current author clock
        self._validate_merge(
            local  = ([(auth_A, 1, cur_ts), (auth_B, 1, cur_ts)], False), 
            remote = ([(auth_A, 1, cur_ts)], False),
            expect = ([(auth_A, 1, cur_ts), (auth_B, 1, cur_ts)], False),
            ancestry = ClockAncestry.LOCAL_MORE_RECENT)

        # remote & local have divergent authors and tick counts
        self._validate_merge(
            local  = ([(auth_A, 2, cur_ts), (auth_C, 2, cur_ts)], False), 
            remote = ([(auth_A, 1, cur_ts), (auth_B, 1, cur_ts)], False),
            expect = ([(auth_A, 2, cur_ts), (auth_B, 1, cur_ts),
                (auth_C, 2, cur_ts)], False),
            ancestry = ClockAncestry.CLOCKS_DIVERGE)

        self._validate_merge(
            local  = ([(auth_A, 1, cur_ts), (auth_B, 1, cur_ts)], False), 
            remote = ([(auth_A, 2, cur_ts), (auth_C, 2, cur_ts)], False),
            expect = ([(auth_A, 2, cur_ts), (auth_B, 1, cur_ts),
                (auth_C, 2, cur_ts)], False),
            ancestry = ClockAncestry.CLOCKS_DIVERGE)

    def test_legacy_merge_scenario_fixtures(self):

        auth_A = ClockUtil.generate_author_id()
        auth_B = ClockUtil.generate_author_id()
        auth_C = ClockUtil.generate_author_id()

        cur_ts = ServerTime.get_time()
        ignore_ts = ServerTime.get_time() - HORIZON
        prune_ts = ignore_ts - ClockUtil.clock_jitter_bound

        # unpruned empty remote & local
        self._validate_merge(
            local  = ([], False),
            remote = ([], False),
            expect = ([], False),
            ancestry = ClockAncestry.CLOCKS_EQUAL)

        # pruned empty remote & local
        self._validate_merge(
            local  = ([], True),
            remote = ([], True),
            expect = ([], True),
            ancestry = ClockAncestry.CLOCKS_EQUAL)

        # pruned empty remote, non-empty ignorable local
        self._validate_merge(
            local  = ([(auth_A, 1, ignore_ts)], False),
            remote = ([], True),
            expect = ([(auth_A, 1, ignore_ts)], False),
            ancestry = ClockAncestry.CLOCKS_EQUAL)

        # non-empty prunable remote, pruned empty local
        self._validate_merge(
            local  = ([], True), 
            remote = ([(auth_A, 1, prune_ts)], False),
            expect = ([], True),
            ancestry = ClockAncestry.CLOCKS_EQUAL)

        # non-empty prunable remote, unpruned empty local
        self._validate_merge(
            local  = ([], False), 
            remote = ([(auth_A, 1, prune_ts)], False),
            expect = ([(auth_A, 1, prune_ts)], False),
            ancestry = ClockAncestry.REMOTE_MORE_RECENT)

        # pruned empty local, unpruned empty remote
        self._validate_merge(
            local  = ([], True),
            remote = ([], False),
            expect = ([], True),
            ancestry = ClockAncestry.LOCAL_MORE_RECENT)

        # unpruned empty local, pruned empty remote
        self._validate_merge(
            local  = ([], False),
            remote = ([], True),
            expect = ([], True),
            ancestry = ClockAncestry.REMOTE_MORE_RECENT) 

        # pruned empty local, unpruned current remote
        self._validate_merge(
            local  = ([], True), 
            remote = ([(auth_A, 1, cur_ts)], False),
            expect = ([(auth_A, 1, cur_ts)], True),
            ancestry = ClockAncestry.CLOCKS_DIVERGE)

        # unpruned current local, pruned empty remote
        self._validate_merge(
            local  = ([(auth_A, 1, cur_ts)], False), 
            remote = ([], True),
            expect = ([(auth_A, 1, cur_ts)], True),
            ancestry = ClockAncestry.CLOCKS_DIVERGE)

        # divergent prunable remote, ignorable local, & common author
        self._validate_merge(
            local  = ([(auth_A, 1, ignore_ts), (auth_C, 3, cur_ts)], True),
            remote = ([(auth_B, 2, prune_ts),  (auth_C, 3, cur_ts)], True),
            expect = ([(auth_A, 1, ignore_ts), (auth_C, 3, cur_ts)], True),
            ancestry = ClockAncestry.CLOCKS_EQUAL)

    def test_merge_datamodel_update(self):

        auth_equal = ClockUtil.generate_author_id()
        auth_remote_prune = ClockUtil.generate_author_id()
        auth_local_prune  = ClockUtil.generate_author_id()
        auth_remote_only = ClockUtil.generate_author_id()
        auth_local_only = ClockUtil.generate_author_id()
        auth_remote_newer_ts = ClockUtil.generate_author_id()
        auth_local_newer_ts = ClockUtil.generate_author_id()
        auth_remote_newer_tick = ClockUtil.generate_author_id()
        auth_local_newer_tick = ClockUtil.generate_author_id()

        cur_ts = ServerTime.get_time()
        ignore_ts = ServerTime.get_time() - HORIZON
        prune_ts = ignore_ts - ClockUtil.clock_jitter_bound

        local_clock = self._build_clock([
            (auth_equal, 1, cur_ts),
            (auth_remote_prune, 1, ignore_ts),
            (auth_local_only, 1, cur_ts),
            (auth_remote_newer_ts, 1, cur_ts - 1),
            (auth_local_newer_ts, 1, cur_ts),
            (auth_remote_newer_tick, 1, cur_ts),
            (auth_local_newer_tick, 2, cur_ts),
            ], True)

        remote_clock = self._build_clock([
            (auth_equal, 1, cur_ts),
            (auth_local_prune, 1, prune_ts),
            (auth_remote_only, 1, cur_ts),
            (auth_remote_newer_ts, 1, cur_ts),
            (auth_local_newer_ts, 1, cur_ts - 1),
            (auth_remote_newer_tick, 2, cur_ts),
            (auth_local_newer_tick, 1, cur_ts),
            ], True)

        expected_merge_steps = {
            auth_equal:        MergeStep.LAUTH_RAUTH_EQUAL,
            auth_remote_prune: MergeStep.RAUTH_PRUNED,
            auth_local_prune:  MergeStep.LAUTH_PRUNED,
            auth_remote_only:  MergeStep.RAUTH_ONLY,
            auth_local_only:   MergeStep.LAUTH_ONLY,
            auth_remote_newer_ts: MergeStep.RAUTH_NEWER,
            auth_local_newer_ts:  MergeStep.LAUTH_NEWER,
            auth_remote_newer_tick: MergeStep.RAUTH_NEWER,
            auth_local_newer_tick: MergeStep.LAUTH_NEWER,
        }

        # given the clock fixtures, validate that we see the
        #  expected ordering of MergeStep values
        expected_merge_steps = [
            v for k, v in sorted(expected_merge_steps.items())]

        def merge_update(merge_step, local_is_legacy, remote_is_legacy):
            self.assertTrue(local_is_legacy)
            self.assertTrue(remote_is_legacy)
            self.assertEquals(merge_step, expected_merge_steps.pop(0))

        ClockUtil.merge(local_clock, remote_clock, HORIZON, merge_update)

    def test_prune(self):

        author_cur = ClockUtil.generate_author_id()
        author_ignore = ClockUtil.generate_author_id()
        author_prune_a = ClockUtil.generate_author_id()
        author_prune_b = ClockUtil.generate_author_id()

        cur_ts = ServerTime.get_time()
        ignore_ts = ServerTime.get_time() - HORIZON
        prune_ts = ignore_ts - ClockUtil.clock_jitter_bound

        record = PersistedRecord()
        record.mutable_cluster_clock().CopyFrom(
            self._build_clock([
                    (author_cur, 1, cur_ts),
                    (author_ignore, 1, ignore_ts),
                    (author_prune_a, 1, prune_ts),
                    (author_prune_b, 1, prune_ts),
                ], False))

        expect = self._build_clock([
                (author_cur, 1, cur_ts),
                (author_ignore, 1, ignore_ts),
            ], True)

        ClockUtil.prune(record, HORIZON, self._prune_noop)

        # assert pruned clock matches expectation
        self.assertEquals(ClockAncestry.CLOCKS_EQUAL,
            ClockUtil.compare(record.cluster_clock, expect))

        record.mutable_cluster_clock().CopyFrom(
            self._build_clock([
                (author_prune_a, 1, prune_ts),
                (author_prune_b, 1, prune_ts),
                ], False))

        ClockUtil.prune(record, HORIZON, self._prune_noop)

        # no clocks remain; cluster_clock has been removed from record
        self.assertFalse(record.has_cluster_clock())

    def test_prune_datamodel_update(self):

        author_prune_a, author_b, author_prune_c, author_d, author_prune_e = \
            sorted(ClockUtil.generate_author_id() for i in xrange(5))

        cur_ts = ServerTime.get_time()
        ignore_ts = ServerTime.get_time() - HORIZON
        prune_ts = ignore_ts - ClockUtil.clock_jitter_bound

        record = PersistedRecord()
        record.mutable_cluster_clock().CopyFrom(
            self._build_clock([
                    (author_prune_a, 1, prune_ts),
                    (author_b, 1, cur_ts),
                    (author_prune_c, 1, prune_ts),
                    (author_d, 1, cur_ts),
                    (author_prune_e, 1, prune_ts),
                ], False))

        expect = self._build_clock([
                (author_b, 1, cur_ts),
                (author_d, 1, cur_ts),
            ], True)

        expected_prune_indices = [
            0, # prune of A
            1, # prune of C, after A's removal
            2, # prune of E, after A & C's removal
        ]

        def prune_update(index):
            self.assertEquals(index, expected_prune_indices.pop(0))

        ClockUtil.prune(record, 1, prune_update)

        # assert pruned clock matches expectation
        self.assertEquals(ClockAncestry.CLOCKS_EQUAL,
            ClockUtil.compare(record.cluster_clock, expect))

    def _tick_noop(self, insert, index):
        pass 

    def _merge_noop(self, local_is_legacy, remote_is_legacy, step):
        pass

    def _prune_noop(self, index):
        pass

