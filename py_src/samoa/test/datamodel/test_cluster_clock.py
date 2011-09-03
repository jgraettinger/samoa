
import random
import unittest

from samoa.core.server_time import ServerTime
from samoa.core.uuid import UUID
from samoa.core.protobuf import ClusterClock, PersistedRecord
from samoa.datamodel.clock_util import ClockUtil, ClockAncestry

class TestClusterClock(unittest.TestCase):

    def test_compare(self):

        A = UUID.from_random()
        B = UUID.from_random()
        C = UUID.from_random()

        clock1 = ClusterClock()
        clock2 = ClusterClock()

        ClockUtil.tick(clock1, A)
        ClockUtil.tick(clock2, A)

        self.assertEquals(ClockAncestry.EQUAL,
            ClockUtil.compare(clock1, clock2))

        ClockUtil.tick(clock2, A)

        # tick of known partitition
        self.assertEquals(ClockAncestry.LESS_RECENT,
            ClockUtil.compare(clock1, clock2))

        ClockUtil.tick(clock1, A)
        ClockUtil.tick(clock2, B)

        # tick of new partition
        self.assertEquals(ClockAncestry.LESS_RECENT,
            ClockUtil.compare(clock1, clock2))

        ClockUtil.tick(clock1, B)

        # should be equal again
        self.assertEquals(ClockAncestry.EQUAL,
            ClockUtil.compare(clock1, clock2))

        ClockUtil.tick(clock1, A)

        # tick of known partition
        self.assertEquals(ClockAncestry.MORE_RECENT,
            ClockUtil.compare(clock1, clock2))

        ClockUtil.tick(clock2, A)
        ClockUtil.tick(clock1, B)

        # tick of new partition
        self.assertEquals(ClockAncestry.MORE_RECENT,
            ClockUtil.compare(clock1, clock2))

        ClockUtil.tick(clock2, B)

        self.assertEquals(ClockAncestry.EQUAL,
            ClockUtil.compare(clock1, clock2))

        # clocks diverge
        ClockUtil.tick(clock1, A)
        ClockUtil.tick(clock2, C)

        self.assertEquals(ClockAncestry.DIVERGE,
            ClockUtil.compare(clock1, clock2))

    def test_merge(self):

        A = UUID.from_random()
        B = UUID.from_random()
        C = UUID.from_random()
        D = UUID.from_random()

        clock1 = ClusterClock()
        clock2 = ClusterClock()
        expect = ClusterClock()

        # a common set of ticks
        ClockUtil.tick(clock1, A)
        ClockUtil.tick(clock1, B)
        ClockUtil.tick(clock2, A)
        ClockUtil.tick(clock2, B)
        ClockUtil.tick(expect, A)
        ClockUtil.tick(expect, B)

        # updates to clock1
        ClockUtil.tick(clock1, A)
        ClockUtil.tick(clock1, C)
        ClockUtil.tick(expect, A)
        ClockUtil.tick(expect, C)

        # updates to clock2
        ClockUtil.tick(clock2, D)
        ClockUtil.tick(clock2, B)
        ClockUtil.tick(expect, D)
        ClockUtil.tick(expect, B)

        # clock1 & clock2 should diverge; build the merged clock
        merged = ClusterClock()
        self.assertEquals(ClockAncestry.DIVERGE,
            ClockUtil.compare(clock1, clock2, 0, merged))

        # the merged clock is equal to the expected clock
        self.assertEquals(ClockAncestry.EQUAL,
            ClockUtil.compare(merged, expect))

    def test_compare_with_horizon(self):

        A_old = UUID.from_random()
        B_old = UUID.from_random()
        C = UUID.from_random()
        D = UUID.from_random()

        clock1 = ClusterClock()
        clock2 = ClusterClock()
        expect = ClusterClock()

        # divergent tick, old enough to be pruned
        ClockUtil.tick(clock1, A_old)

        ServerTime.set_time(ServerTime.get_time() \
            + ClockUtil.clock_jitter_bound)

        # divergent tick, ignored but _not_ pruned
        ClockUtil.tick(clock2, B_old)
        ClockUtil.tick(expect, B_old)

        ServerTime.set_time(ServerTime.get_time() + 1)

        # common ticks
        ClockUtil.tick(clock1, C)
        ClockUtil.tick(clock2, C)
        ClockUtil.tick(expect, C)

        # clock1 & clock2 should be equal with a horizon of 1
        merged = ClusterClock()
        self.assertEquals(ClockAncestry.EQUAL,
            ClockUtil.compare(clock1, clock2, 1, merged))

        # the merged clock is exactly equal to the expected clock
        #  => B_old is present, but A_old is not
        self.assertEquals(ClockAncestry.EQUAL,
            ClockUtil.compare(merged, expect))

        # divergent ticks
        ClockUtil.tick(clock1, C)
        ClockUtil.tick(expect, C)

        ClockUtil.tick(clock2, D)
        ClockUtil.tick(expect, D)

        # clock1 & clock2 should be divergent with a horizon of 1
        merged = ClusterClock()
        self.assertEquals(ClockAncestry.DIVERGE,
            ClockUtil.compare(clock1, clock2, 1, merged))

        # the merged clock is exactly equal to the expected clock
        self.assertEquals(ClockAncestry.EQUAL,
            ClockUtil.compare(merged, expect))

    def test_prune_record(self):

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

    def test_serialization(self):

        A = UUID.from_random()
        B = UUID.from_random()
        C = UUID.from_random()

        clock = ClusterClock()

        ClockUtil.tick(clock, A)
        ClockUtil.tick(clock, A)
        ClockUtil.tick(clock, A)
        ClockUtil.tick(clock, B)
        ClockUtil.tick(clock, B)
        ClockUtil.tick(clock, C)

        serialized_clock = clock.SerializeToBytes()

        recovered = ClusterClock()
        recovered.ParseFromBytes(serialized_clock)

        self.assertEquals(ClockAncestry.EQUAL,
            ClockUtil.compare(clock, recovered))

