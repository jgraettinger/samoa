package rollingHash

import (
	. "launchpad.net/gocheck"
	"testing"
)

type PacketTest struct {
}

func (s *PacketTest) TestAlignment(c *C) {
	c.Check(uint32(4), Equals, packetAlignment)
	c.Check(uint32(13), Equals, headerByteLength)
	c.Check(uint32(1), Equals, headerAlignment)
	c.Check(uint32(3), Equals, capacityAlignmentAdjustment)
	c.Check(uint32(8191), Equals, maxCapacity)
}

func (s *PacketTest) TestFlags(c *C) {
	var p packet
	p.markDead()
	p.markContinuesSequence()
	p.markCompletesSequence()
	c.Check(p.isDead(), Equals, true)
	c.Check(p.continuesSequence(), Equals, true)
	c.Check(p.completesSequence(), Equals, true)
}

func (s *PacketTest) TestInitialize(c *C) {
	var p packet
	c.Check(p.capacity(), Equals, capacityAlignmentAdjustment)

	c.Check(func() { p.initialize(capacityAlignmentAdjustment + 1) },
		PanicMatches, "Capacity of \\d is not \\d-aligned.")
	c.Check(func() { p.initialize(1 << 31) },
		PanicMatches, "Capacity of \\d+ is larger than max capacity \\d+.")

	// Smallest possible packet.
	p.initialize(capacityAlignmentAdjustment)
	c.Check(p.capacity(), Equals, capacityAlignmentAdjustment)
	c.Check(p.availableCapacity(), Equals, capacityAlignmentAdjustment)

	p.markDead()
	p.markContinuesSequence()
	p.markCompletesSequence()
	c.Check(p.capacity(), Equals, capacityAlignmentAdjustment)

	// Largest possible packet.
	p.initialize(maxCapacity)
	c.Check(p.capacity(), Equals, maxCapacity)
	c.Check(p.availableCapacity(), Equals, maxCapacity)

	p.markDead()
	p.markContinuesSequence()
	p.markCompletesSequence()
	c.Check(p.capacity(), Equals, maxCapacity)
}

func (s *PacketTest) TestKeyAndValue(c *C) {
	caa := capacityAlignmentAdjustment
	var p packet
	p.initialize(40 + caa)
	c.Check(p.availableCapacity(), Equals, 40 + caa)

	c.Check(func() { p.setKey(41 + caa) },
		PanicMatches, "Key length \\d+ is too long for packet{[^}]+}.")
	c.Check(func() { p.setValue(41 + caa) },
		PanicMatches, "Value length \\d+ is too long for packet{[^}]+}.")

	copy(p.setKey(8), []byte("feedbeef"))
	c.Check(p.keyLength(), Equals, uint32(8))
	c.Check(p.key(), DeepEquals, []byte("feedbeef"))
	c.Check(p.valueLength(), Equals, uint32(0))
	c.Check(p.availableCapacity(), Equals, 32 + caa)

	c.Check(func() { p.setKey(8) },
		PanicMatches, "packet{[^}]+} is not empty.")
	c.Check(func() { p.setValue(33 + caa) },
		PanicMatches, "Value length \\d+ is too long for packet{[^}]+}.")

	copy(p.setValue(12), []byte("hello, world"))
	c.Check(p.valueLength(), Equals, uint32(12))
	c.Check(p.value(), DeepEquals, []byte("hello, world"))
	c.Check(p.key(), DeepEquals, []byte("feedbeef"))
	c.Check(p.availableCapacity(), Equals, 20 + caa)
	c.Check(func() { p.setValue(33 + caa) },
		PanicMatches, "Value length \\d+ is too long for packet{[^}]+}.")

	copy(p.setValue(32 + caa), []byte("goodbye, world"))
	c.Check(p.value()[:14], DeepEquals, []byte("goodbye, world"))
	c.Check(p.key(), DeepEquals, []byte("feedbeef"))
	c.Check(p.valueLength(), Equals, 32 + caa)
	c.Check(p.availableCapacity(), Equals, uint32(0))

	// No flag bits were harmed in the making of this test.
	c.Check(p.isDead(), Equals, false)
	c.Check(p.continuesSequence(), Equals, false)
	c.Check(p.completesSequence(), Equals, false)
}

func Test(t *testing.T) { TestingT(t) }

var _ = Suite(&PacketTest{})
