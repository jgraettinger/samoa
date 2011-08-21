
import random
import unittest

from samoa.core.uuid import UUID
from samoa.core.protobuf import ClusterClock
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
            ClockUtil.compare(clock1, clock2, merged))

        # the merged clock is equal to the expected clock
        self.assertEquals(ClockAncestry.EQUAL,
            ClockUtil.compare(merged, expect))

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

