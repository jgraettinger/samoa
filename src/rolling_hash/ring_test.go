package rollingHash

import (
	. "launchpad.net/gocheck"
	"testing"
)

type RingTest struct {
}

func (s *RingTest) TestAllocateAndReclaimWithFixture(c *C) {
		var ring Ring
		ring.initialize(make([]byte, 16572), 1)

		c.Assert(ring.storageOffset(), Equals, 20)
		c.Assert(ring.storageSize(), Equals, 16552)

		// Allocation #1: 13 of header, 103 of capacity (3 of padding).
		pkt := ring.allocatePackets(100)
        c.Assert(pkt.packetLength(), Equals, 116)
		c.Assert(pkt.capacity(), Equals, 103)
        c.Assert(pkt.continuesSequence(), Equals, false)
		c.Assert(pkt.completesSequence(), Equals, true)
		c.Assert(ring.nextPacket(pkt), IsNil)
        c.Assert(ring.header.end, Equals, 136)

        // Allocation #2: requires two packets.
        // First packet has 13 of header, 8191 of capacity.
        pkt = ring.allocatePackets(8193)
        c.Assert(pkt.packetLength(), Equals, 8204)
        c.Assert(pkt.capacity(), Equals, 8191)
        c.Assert(pkt.continuesSequence(), Equals, false)
        c.Assert(pkt.completesSequence(), Equals, false)

        // Second packet has 13 of header, 3 of capacity (2 of padding).
        pkt = ring.nextPacket(pkt)
		c.Assert(pkt.packetLength(), Equals, 16)
        c.Assert(pkt.capacity(), Equals, 3)
        c.Assert(pkt.continuesSequence(), Equals, true)
        c.Assert(pkt.completesSequence(), Equals, true)
        c.Assert(ring.nextPacket(pkt), IsNil)
        c.Assert(ring.header.end, Equals, 8356)
        c.Assert(ring.storageUsed(), Equals, 8336)

        // Allocation which fails (one byte to large).
        c.Assert(ring.allocatePackets(8191), IsNil)
        c.Assert(ring.header.end, Equals, 8356)

        // Allocation #3: could fit in one packet,
        // but is split into two so as not to leave a remainder
        // less than a minimum-length packet in size.
        // First packet has 13 of header, 8187 of capacity.
        pkt = ring.allocatePackets(8190)
        c.Assert(pkt.packetLength(), Equals, 8200)
        c.Assert(pkt.capacity(), Equals, 8187)
        c.Assert(pkt.continuesSequence(), Equals, false)
        c.Assert(pkt.completesSequence(), Equals, false)

        // Second packet has 13 of header, 3 of capacity.
        pkt = ring.nextPacket(pkt)
        c.Assert(pkt.packetLength(), Equals, 16)
        c.Assert(pkt.capacity(), Equals, 3)
        c.Assert(pkt.continuesSequence(), Equals, true)
        c.Assert(pkt.completesSequence(), Equals, true)

		// The ring's storage is entirely used.
		c.Assert(ring.header.wrapped, Equals, true)
        c.Assert(ring.header.begin, Equals, ring.header.end)
        c.Assert(ring.storageUsed(), Equals, 16552)

        // Free allocation #1.
        ring.head().markDead()
        c.Assert(ring.reclaimHead(), Equals, 116)
        c.Assert(ring.header.begin, Equals, 136)
        c.Assert(ring.storageUsed(), Equals, 16436)

        // Allocation #4: sized into a larger packet,
        // so as not to leave a remainder less than a
        // minimum-length packet in size.
        // Packet has 13 of header, 103 of capacity (12 of padding).
        pkt = ring.allocatePackets(91)
		c.Assert(pkt.packetLength(), Equals, 116)
		c.Assert(pkt.capacity(), Equals, 103)
        c.Assert(pkt.continuesSequence(), Equals, false)
        c.Assert(pkt.completesSequence(), Equals, true)
        c.Assert(ring.header.begin, Equals, 136)
        c.Assert(ring.header.end, Equals, 136)

        // Reclaim both packets from allocation #2.
        ring.head().markDead()
        ring.nextPacket(ring.head()).markDead()
        c.Assert(ring.reclaimHead(), Equals, 8220)
        c.Assert(ring.header.begin, Equals, 8356)
        c.Assert(ring.header.end, Equals, 136)
        c.Assert(ring.header.wrapped, Equals, true)

        // Reclaim both packets from allocation #3. The ring unwraps.
        ring.head().markDead()
        ring.nextPacket(ring.head()).markDead()
        c.Assert(ring.reclaimHead(), Equals, 8216)
        c.Assert(ring.header.begin, Equals, ring.storageOffset())
        c.Assert(ring.header.wrapped, Equals, false)

		// Allocation #5: 13 of header, 3967 of capacity (0 of padding).
        pkt = ring.allocatePackets(3967)
		c.Assert(pkt.packetLength(), Equals, 3980)
        c.Assert(pkt.capacity(), Equals, 3967)
        c.Assert(pkt.continuesSequence(), Equals, false)
        c.Assert(pkt.completesSequence(), Equals, true)
        c.Assert(ring.header.begin, Equals, 20)
        c.Assert(ring.header.end, Equals, 4116)
        c.Assert(ring.storageUsed(), Equals, 4096)

        // Recmlaim allocation #4.
        ring.head().markDead()
        c.Assert(ring.reclaimHead(), Equals, 116)
        c.Assert(ring.header.begin, Equals, 136)
        c.Assert(ring.header.end, Equals, 4116)
        c.Assert(ring.header.wrapped, Equals, false)

        // Reclaim allocation #5. Ring is now empty.
        ring.head().markDead()
        c.Assert(ring.reclaimHead(), Equals, 3980)
        c.Assert(ring.header.begin, Equals, 4116)
        c.Assert(ring.header.end, Equals, 4116)
        c.Assert(ring.header.wrapped, Equals, false)
        c.Assert(ring.storageUsed(), Equals, 0)
}

func TestRing(t *testing.T) { TestingT(t) }

var _ = Suite(&RingTest{})
