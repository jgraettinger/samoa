package rollingHash

import (
	. "launchpad.net/gocheck"
	"testing"
	"unsafe"
)

type RingTest struct {
}

func (s *RingTest) TestConstants(c *C) {
	c.Check(kRingHeaderByteLength, Equals, 32)
	c.Check(kRingBulkheadShift, Equals, 20)
	c.Check(kUint32ByteLength, Equals, 4)
}

func (s *RingTest) TestInitialize(c *C) {
	var ring Ring
	ring.initialize(make([]byte, 4096), 3)
	c.Assert(ring.region, HasLen, 4096)
	c.Assert(ring.index, HasLen, 3)
	// Ring index is immediately after the ring header.
	c.Assert(unsafe.Pointer(&ring.index[0]), Equals,
		unsafe.Pointer(&ring.region[kRingHeaderByteLength]))
	// Ring storage is after the index, but aligned to kPacketAlignment.
	c.Assert(ring.storageOffset(), Equals,
		kRingHeaderByteLength+kUint32ByteLength*4)
}

func (s *RingTest) TestAllocateAndReclaimWithFixture(c *C) {
	var ring Ring
	ring.initialize(make([]byte, 16608), 1)

	c.Assert(ring.storageOffset(), Equals, 40)
	c.Assert(ring.storageSize(), Equals, 16568)

	// Allocation #1: 17 of header, 103 of capacity (3 of padding).
	pkt := ring.allocatePackets(100)
	c.Assert(pkt.packetLength(), Equals, 120)
	c.Assert(pkt.capacity(), Equals, 103)
	c.Assert(pkt.continuesSequence(), Equals, false)
	c.Assert(pkt.completesSequence(), Equals, true)
	c.Assert(ring.nextPacket(pkt), IsNil)
	c.Assert(ring.end(), Equals, 160)

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
	c.Assert(ring.end(), Equals, 8392)
	c.Assert(ring.storageUsed(), Equals, 8352)

	// Allocation which fails. As is, the packet would leave a remainder
	// less than a minimum-length packet. If rounded up, the capacity
	// would be too large for a single packet. When broken into two packets,
	// it's 1 byte too large for remaining ring storage capacity.
	c.Assert(ring.allocatePackets(8183), IsNil)
	c.Assert(ring.end(), Equals, 8392)

	// Allocation #3: could fit in one packet, but is split into two so as
	// not to leave a remainder less than a minimum-length packet in size.
	// First packet has 17 of header, 8175 of capacity.
	pkt = ring.allocatePackets(8181)
	c.Assert(pkt.packetLength(), Equals, 8192)
	c.Assert(pkt.capacity(), Equals, 8175)
	c.Assert(pkt.continuesSequence(), Equals, false)
	c.Assert(pkt.completesSequence(), Equals, false)

	// Second packet has 17 of header, 7 of capacity (1 of padding).
	pkt = ring.nextPacket(pkt)
	c.Assert(pkt.packetLength(), Equals, 24)
	c.Assert(pkt.capacity(), Equals, 7)
	c.Assert(pkt.continuesSequence(), Equals, true)
	c.Assert(pkt.completesSequence(), Equals, true)

	// The ring's storage is entirely used.
	c.Assert(ring.begin(), Equals, 40)
	c.Assert(ring.end(), Equals, 40)
	c.Assert(ring.wrapped(), Equals, true)
	c.Assert(ring.storageUsed(), Equals, 16568)

	// Free allocation #1.
	ring.head().markDead()
	c.Assert(ring.reclaimHead(), Equals, 120)
	c.Assert(ring.begin(), Equals, 160)
	c.Assert(ring.end(), Equals, 40)
	c.Assert(ring.storageUsed(), Equals, 16448)

	// Allocation #4: sized into a much larger packet, so as not to leave a
	// remainder less than a minimum-length packet in size.
	// Packet has 17 of header, 103 of capacity (23 of padding).
	pkt = ring.allocatePackets(80)
	c.Assert(pkt.packetLength(), Equals, 120)
	c.Assert(pkt.capacity(), Equals, 103)
	c.Assert(pkt.continuesSequence(), Equals, false)
	c.Assert(pkt.completesSequence(), Equals, true)
	c.Assert(ring.begin(), Equals, 160)
	c.Assert(ring.end(), Equals, 160)

	// Reclaim both packets from allocation #2.
	ring.head().markDead()
	ring.nextPacket(ring.head()).markDead()
	c.Assert(ring.reclaimHead(), Equals, 8232)
	c.Assert(ring.begin(), Equals, 8392)
	c.Assert(ring.end(), Equals, 160)
	c.Assert(ring.wrapped(), Equals, true)

	// Reclaim both packets from allocation #3. The ring unwraps.
	ring.head().markDead()
	ring.nextPacket(ring.head()).markDead()
	c.Assert(ring.reclaimHead(), Equals, 8216)
	c.Assert(ring.begin(), Equals, ring.storageOffset())
	c.Assert(ring.wrapped(), Equals, false)

	// Allocation #5: 17 of header, 3967 of capacity (0 of padding).
	pkt = ring.allocatePackets(3967)
	c.Assert(pkt.packetLength(), Equals, 3984)
	c.Assert(pkt.capacity(), Equals, 3967)
	c.Assert(pkt.continuesSequence(), Equals, false)
	c.Assert(pkt.completesSequence(), Equals, true)
	c.Assert(ring.begin(), Equals, 40)
	c.Assert(ring.end(), Equals, 4144)
	c.Assert(ring.storageUsed(), Equals, 4104)

	// Reclaim allocation #4.
	ring.head().markDead()
	c.Assert(ring.reclaimHead(), Equals, 120)
	c.Assert(ring.begin(), Equals, 160)
	c.Assert(ring.end(), Equals, 4144)
	c.Assert(ring.wrapped(), Equals, false)

	// Reclaim allocation #5. Ring is now empty.
	ring.head().markDead()
	c.Assert(ring.reclaimHead(), Equals, 3984)
	c.Assert(ring.begin(), Equals, 4144)
	c.Assert(ring.end(), Equals, 4144)
	c.Assert(ring.wrapped(), Equals, false)
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
