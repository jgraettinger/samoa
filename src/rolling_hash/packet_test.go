package rollingHash

import (
	. "launchpad.net/gocheck"
	"testing"
)

type PacketTest struct {
}

func (s *PacketTest) TestConstants(c *C) {
	c.Check(8, Equals, kPacketAlignment)
	c.Check(17, Equals, kPacketHeaderByteLength)
	c.Check(1, Equals, kPacketHeaderAlignment)
	c.Check(7, Equals, kPacketAlignmentAdjustment)
	c.Check(8191, Equals, kPacketMaxCapacity)
	c.Check(24, Equals, kPacketMinByteLength)
	c.Check(8208, Equals, kPacketMaxByteLength)
}

func (s *PacketTest) TestInitializeAndFlags(c *C) {
	var p packet
	// Two lowest bits of packet are implicit.
	c.Check(p.capacity(), Equals, 3)

	c.Check(func() { p.initialize(kPacketMinByteLength + 1) },
		PanicMatches, "Length of \\d+ is not packet aligned.")
	c.Check(func() { p.initialize(1 << 30) },
		PanicMatches, "\\d+ overflows max packet length.")

	// Smallest possible packet.
	p.initialize(kPacketMinByteLength)
	c.Check(p.capacity(), Equals, kPacketAlignmentAdjustment)
	c.Check(p.availableCapacity(), Equals, kPacketAlignmentAdjustment)

	c.Check(p.isDead(), Equals, false)
	c.Check(p.continuesSequence(), Equals, false)
	c.Check(p.completesSequence(), Equals, false)

	p.markDead()
	p.markContinuesSequence()
	p.markCompletesSequence()

	c.Check(p.capacity(), Equals, kPacketAlignmentAdjustment)
	c.Check(p.availableCapacity(), Equals, kPacketAlignmentAdjustment)
	c.Check(p.isDead(), Equals, true)
	c.Check(p.continuesSequence(), Equals, true)
	c.Check(p.completesSequence(), Equals, true)
	c.Check(p.keyLength(), Equals, 0)
	c.Check(p.valueLength(), Equals, 0)

	// Largest possible packet.
	p.initialize(kPacketMaxByteLength)
	c.Check(p.capacity(), Equals, kPacketMaxCapacity)
	c.Check(p.availableCapacity(), Equals, kPacketMaxCapacity)
	c.Check(p.isDead(), Equals, false)
	c.Check(p.continuesSequence(), Equals, false)
	c.Check(p.completesSequence(), Equals, false)

	p.markDead()
	p.markContinuesSequence()
	p.markCompletesSequence()

	c.Check(p.capacity(), Equals, kPacketMaxCapacity)
	c.Check(p.availableCapacity(), Equals, kPacketMaxCapacity)
	c.Check(p.isDead(), Equals, true)
	c.Check(p.continuesSequence(), Equals, true)
	c.Check(p.completesSequence(), Equals, true)
	c.Check(p.keyLength(), Equals, 0)
	c.Check(p.valueLength(), Equals, 0)
}

func (s *PacketTest) TestKeyAndValue(c *C) {
	paa := kPacketAlignmentAdjustment
	var p packet
	p.initialize(kPacketHeaderByteLength + 40 + paa)
	c.Check(p.availableCapacity(), Equals, 40+paa)

	c.Check(func() { p.setKey(41 + paa) },
		PanicMatches, "Key length \\d+ is too long for packet{[^}]+}.")
	c.Check(func() { p.setValue(41 + paa) },
		PanicMatches, "Value length \\d+ is too long for packet{[^}]+}.")

	copy(p.setKey(8), []byte("feedbeef"))
	c.Check(p.keyLength(), Equals, 8)
	c.Check(p.key(), DeepEquals, []byte("feedbeef"))
	c.Check(p.valueLength(), Equals, 0)
	c.Check(p.availableCapacity(), Equals, 32+paa)

	c.Check(func() { p.setKey(8) },
		PanicMatches, "packet{[^}]+} is not empty.")
	c.Check(func() { p.setValue(33 + paa) },
		PanicMatches, "Value length \\d+ is too long for packet{[^}]+}.")

	copy(p.setValue(12), []byte("hello, world"))
	c.Check(p.valueLength(), Equals, 12)
	c.Check(p.value(), DeepEquals, []byte("hello, world"))
	c.Check(p.key(), DeepEquals, []byte("feedbeef"))
	c.Check(p.availableCapacity(), Equals, 20+paa)
	c.Check(func() { p.setValue(33 + paa) },
		PanicMatches, "Value length \\d+ is too long for packet{[^}]+}.")

	copy(p.setValue(32+paa), []byte("goodbye, world"))
	c.Check(p.value()[:14], DeepEquals, []byte("goodbye, world"))
	c.Check(p.key(), DeepEquals, []byte("feedbeef"))
	c.Check(p.valueLength(), Equals, 32+paa)
	c.Check(p.availableCapacity(), Equals, 0)

	// No flag bits were harmed in the making of this test.
	c.Check(p.isDead(), Equals, false)
	c.Check(p.continuesSequence(), Equals, false)
	c.Check(p.completesSequence(), Equals, false)
}

func TestPacket(t *testing.T) { TestingT(t) }

var _ = Suite(&PacketTest{})
