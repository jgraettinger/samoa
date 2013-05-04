package rollingHash

import (
	"hash/crc64"
	. "launchpad.net/gocheck"
	"testing"
)

type PacketTest struct {
}

func (s *PacketTest) TestConstants(c *C) {
	c.Check(kPacketAlignment, Equals, 8)
	c.Check(kPacketHeaderByteLength, Equals, 17)
	c.Check(kPacketHeaderAlignment, Equals, 1)
	c.Check(kPacketAlignmentAdjustment, Equals, 7)
	c.Check(kPacketMaxCapacity, Equals, 8191)
	c.Check(kPacketMinByteLength, Equals, 24)
	c.Check(kPacketMaxByteLength, Equals, 8208)
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

func (s *PacketTest) TestIntegretyChecking(c *C) {
	var p1, p2 packet

	// Uninitialized packets have invalid CRCs.
	c.Check(p1.checkIntegrity(crc64.New(kPacketChecksumTable)),
		ErrorMatches, "Checksum mismatch")

	// Key/value lengths are checked for consistency with capacity
	// before attempting to read key/value content.
	p1.initialize(48)
	p1.meta[2] = 0xff
	c.Check(p1.checkIntegrity(crc64.New(kPacketChecksumTable)),
		ErrorMatches, "Invalid key/value length")

	p1.initialize(1000)
	p2.initialize(1000)

	copy(p1.setKey(8), []byte("test-key"))
	copy(p1.setValue(10), []byte("test-value"))
	p2.markContinuesSequence()
	p2.markCompletesSequence()
	copy(p2.setValue(17), []byte("test-value-part-2"))

	// The un-initialized CRC will mismatch.
	contentSummer := crc64.New(kPacketChecksumTable)
	c.Check(p1.checkIntegrity(contentSummer),
		ErrorMatches, "Checksum mismatch")
	c.Check(p2.checkIntegrity(contentSummer),
		ErrorMatches, "Checksum mismatch")

	// Compute and set correct packet CRC's.
	contentSummer = crc64.New(kPacketChecksumTable)
	p1.crc = p1.computeCombinedChecksum(contentSummer)
	p2.crc = p2.computeCombinedChecksum(contentSummer)

	// Packet integrety checks now pass.
	contentSummer = crc64.New(kPacketChecksumTable)
	c.Check(p1.checkIntegrity(contentSummer), IsNil)
	c.Check(p2.checkIntegrity(contentSummer), IsNil)

	// A check of p2 alone fails, as it's dependent on p1's content.
	contentSummer = crc64.New(kPacketChecksumTable)
	c.Check(p2.checkIntegrity(contentSummer),
		ErrorMatches, "Checksum mismatch")

	// Update p1's metadata.
	oldMetaChecksum := p1.computeMetaChecksum()
	p1.next = 1234
	c.Check(p1.computeMetaChecksum(), Not(Equals), oldMetaChecksum)
	p1.updateMetaOfCombinedChecksum(oldMetaChecksum)

	// Packet integrety checks still pass.
	contentSummer = crc64.New(kPacketChecksumTable)
	c.Check(p1.checkIntegrity(contentSummer), IsNil)
	c.Check(p2.checkIntegrity(contentSummer), IsNil)

	// Modify p1 content without updating the checksum. Both checks now fail.
	copy(p1.setValue(10), []byte("test-valu3"))
	contentSummer = crc64.New(kPacketChecksumTable)
	c.Check(p1.checkIntegrity(contentSummer),
		ErrorMatches, "Checksum mismatch")
	c.Check(p2.checkIntegrity(contentSummer),
		ErrorMatches, "Checksum mismatch")
}

func (s *PacketTest) TestContentSumEquivalence(c *C) {
	var p1, p2, p3 packet

	p1.initialize(96)
	copy(p1.setKey(8), []byte("test-key"))
	copy(p1.setValue(10), []byte("test-value"))
	summer1 := crc64.New(kPacketChecksumTable)
	p1.computeContentChecksum(summer1)

	p1.initialize(96)
	p2.initialize(96)
	copy(p1.setKey(8), []byte("test-key"))
	copy(p2.setValue(10), []byte("test-value"))
	summer2 := crc64.New(kPacketChecksumTable)
	p1.computeContentChecksum(summer2)
	p2.computeContentChecksum(summer2)

	p1.initialize(96)
	p2.initialize(96)
	p3.initialize(96)
	copy(p1.setKey(6), []byte("test-k"))
	copy(p2.setKey(2), []byte("ey"))
	copy(p2.setValue(3), []byte("tes"))
	copy(p3.setValue(7), []byte("t-value"))
	summer3 := crc64.New(kPacketChecksumTable)
	p1.computeContentChecksum(summer3)
	p2.computeContentChecksum(summer3)
	p3.computeContentChecksum(summer3)

	// Despite different allocations of key and value content,
	// the resulting content checksums are identical.
	c.Check(summer1.Sum64(), Equals, summer2.Sum64())
	c.Check(summer1.Sum64(), Equals, summer3.Sum64())
}

func TestPacket(t *testing.T) { TestingT(t) }

var _ = Suite(&PacketTest{})
