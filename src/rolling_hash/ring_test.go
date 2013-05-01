package rollingHash

import (
	. "launchpad.net/gocheck"
	"testing"
	"unsafe"
)

type RingTest struct {
}

func (s *RingTest) TestInitialize(c *C) {
	var ring Ring
	ring.initialize(make([]byte, 4096), 10)
	c.Assert(ring.region, HasLen, 4096)
	c.Assert(ring.index, HasLen, 10)
	// Ring index is immediately after the ring header.
	c.Assert(unsafe.Pointer(&ring.index[0]), Equals,
		unsafe.Pointer(&ring.region[kRingHeaderByteLength]))
	// Ring storage is immediately after the index.
	c.Assert(ring.storageOffset(), Equals,
		kRingHeaderByteLength+kUint32ByteLength*10)
}

func (s *RingTest) TestAllocateAndReclaimWithFixture(c *C) {
	var ring Ring
	ring.initialize(make([]byte, 16572), 1)

	c.Assert(ring.storageOffset(), Equals, 20)
	c.Assert(ring.storageSize(), Equals, 16552)

	// Allocation #1: 17 of header, 103 of capacity (3 of padding).
	pkt := ring.allocatePackets(100)
	c.Assert(pkt.packetLength(), Equals, 120)
	c.Assert(pkt.capacity(), Equals, 103)
	c.Assert(pkt.continuesSequence(), Equals, false)
	c.Assert(pkt.completesSequence(), Equals, true)
	c.Assert(ring.nextPacket(pkt), IsNil)
	c.Assert(ring.header.end, Equals, 140)

	// Allocation #2: requires two packets.
	// First packet has 17 of header, 8191 of capacity.
	pkt = ring.allocatePackets(8193)
	c.Assert(pkt.packetLength(), Equals, 8208)
	c.Assert(pkt.capacity(), Equals, 8191)
	c.Assert(pkt.continuesSequence(), Equals, false)
	c.Assert(pkt.completesSequence(), Equals, false)

	// Second packet has 17 of header, 7 of capacity (5 of padding).
	pkt = ring.nextPacket(pkt)
	c.Assert(pkt.packetLength(), Equals, 24)
	c.Assert(pkt.capacity(), Equals, 7)
	c.Assert(pkt.continuesSequence(), Equals, true)
	c.Assert(pkt.completesSequence(), Equals, true)
	c.Assert(ring.nextPacket(pkt), IsNil)
	c.Assert(ring.header.end, Equals, 8372)
	c.Assert(ring.storageUsed(), Equals, 8352)

	// Allocation which fails (one byte to large for remaining ring capacity).
	c.Assert(ring.allocatePackets(8184), IsNil)
	c.Assert(ring.header.end, Equals, 8372)

	// Allocation #3: could fit in one packet,
	// but is split into two so as not to leave a remainder
	// less than a minimum-length packet in size.
	// First packet has 13 of header, 8187 of capacity.
	pkt = ring.allocatePackets(8160)
	c.Assert(pkt.packetLength(), Equals, 8200)
	c.Assert(pkt.capacity(), Equals, 8183)
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

func (s *RingTest) TestBulkheadBoundariesWithFixture(c *C) {
	var ring Ring
	ring.initialize(make([]byte, 1<<(kRingBulkheadShift+1)), 1)

	c.Assert(ring.allocatePackets(
		(1<<kRingBulkheadShift)-kPacketMaxByteLength), NotNil)

	// Allocation is split into two packets, on either side of bulkhead.
	head := ring.allocatePackets(kPacketMaxByteLength)
	tail := ring.nextPacket(head)
	c.Assert(head.completesSequence(), Equals, false)
	c.Assert(tail.continuesSequence(), Equals, true)
	c.Assert(tail.completesSequence(), Equals, true)
	// Tail's offset is aligned to bulkhead boundary.
	c.Assert(ring.packetOffset(tail)%(1<<kRingBulkheadShift), Equals, 0)
}

func TestRing(t *testing.T) { TestingT(t) }

var _ = Suite(&RingTest{})
