
import random
import unittest

from samoa.core.uuid import UUID
from samoa.datamodel.cluster_clock import ClusterClock, ClockAncestry

class TestClusterClock(unittest.TestCase):

    def test_compare(self):

        A = UUID.from_random()
        B = UUID.from_random()
        C = UUID.from_random()

        clock1 = ClusterClock()
        clock2 = ClusterClock()

        clock1.tick(A)
        clock2.tick(A)

        self.assertEquals(ClockAncestry.EQUAL,
            ClusterClock.compare(clock1, clock2))

        clock2.tick(A)

        # tick of known partitition
        self.assertEquals(ClockAncestry.LESS_RECENT,
            ClusterClock.compare(clock1, clock2))

        clock1.tick(A)
        clock2.tick(B)

        # tick of new partition
        self.assertEquals(ClockAncestry.LESS_RECENT,
            ClusterClock.compare(clock1, clock2))

        clock1.tick(B)

        # should be equal again
        self.assertEquals(ClockAncestry.EQUAL,
            ClusterClock.compare(clock1, clock2))

        clock1.tick(A)

        # tick of known partition
        self.assertEquals(ClockAncestry.MORE_RECENT,
            ClusterClock.compare(clock1, clock2))

        clock2.tick(A)
        clock1.tick(B)

        # tick of new partition
        self.assertEquals(ClockAncestry.MORE_RECENT,
            ClusterClock.compare(clock1, clock2))

        clock2.tick(B)

        self.assertEquals(ClockAncestry.EQUAL,
            ClusterClock.compare(clock1, clock2))

        # clocks diverge
        clock1.tick(A)
        clock2.tick(C)

        self.assertEquals(ClockAncestry.DIVERGE,
            ClusterClock.compare(clock1, clock2))

    def test_merge(self):

        A = UUID.from_random()
        B = UUID.from_random()
        C = UUID.from_random()
        D = UUID.from_random()

        clock1 = ClusterClock()
        clock2 = ClusterClock()
        expect = ClusterClock()

        # a common set of ticks
        clock1.tick(A)
        clock1.tick(B)
        clock2.tick(A)
        clock2.tick(B)
        expect.tick(A)
        expect.tick(B)

        # updates to clock1
        clock1.tick(A)
        clock1.tick(C)
        expect.tick(A)
        expect.tick(C)

        # updates to clock2
        clock2.tick(D)
        clock2.tick(B)
        expect.tick(D)
        expect.tick(B)

        # clock1 & clock2 should diverge; build the merged clock
        merged = ClusterClock()
        self.assertEquals(ClockAncestry.DIVERGE,
            ClusterClock.compare(clock1, clock2, merged))

        # the merged clock is equal to the expected clock
        self.assertEquals(ClockAncestry.EQUAL,
            ClusterClock.compare(merged, expect))

    def test_serialization(self):

        A = UUID.from_random()
        B = UUID.from_random()
        C = UUID.from_random()

        clock = ClusterClock()

        clock.tick(A)
        clock.tick(A)
        clock.tick(A)
        clock.tick(B)
        clock.tick(B)
        clock.tick(C)

        serialized_clock = str(clock)

        recovered = ClusterClock.from_string(serialized_clock)

        self.assertEquals(ClockAncestry.EQUAL,
            ClusterClock.compare(clock, recovered))

