package rollingHash

import (
	"fmt"
	"hash"
	"hash/crc64"
	"strings"
	"unsafe"
)

type packet struct {
	crc  uint64
	next uint32
	meta [5]uint8
	data [1 << 13]byte
}

const (
	// Placement of packets within a ring must adhere to the packet
	// byte-alignment (to do otherwise would result in a bus error).
	// Alignment will be 4 bytes on most architectures.
	kPacketAlignment = int(unsafe.Alignof(packet{}))
	// Byte-length of the packet metadata header.
	kPacketHeaderByteLength = int(unsafe.Offsetof(packet{}.data))
	// Byte-alignment of the packet header.
	kPacketHeaderAlignment = kPacketHeaderByteLength % kPacketAlignment
	// Byte-alignment of packet capacity required to correct for
	// the header alignment, ensuring the total packet length
	// obeys the packet alignment.
	kPacketAlignmentAdjustment = kPacketAlignment - kPacketHeaderAlignment
	// The capacity field is 13 logical bits and 11 stored bits.
	// Alignment imposes the value of the 2 lower bits, and
	// they aren't actually stored.
	kPacketMaxCapacity = 1<<13 - kPacketHeaderAlignment

	kPacketMinByteLength = kPacketHeaderByteLength + kPacketAlignmentAdjustment
	kPacketMaxByteLength = kPacketHeaderByteLength + kPacketMaxCapacity

	kPacketDeadMask         = 0x80 // meta[0] 0b10000000
	kPacketContinuesMask    = 0x40 // meta[0] 0b01000000
	kPacketCompletesMask    = 0x20 // meta[0] 0b00100000
	kPacketCapacityMask1    = 0x1f // meta[0] 0b00011111 (high bits)
	kPacketCapacityMask2    = 0xfc // meta[1] 0b11111100 (low bits)
	kPacketKeyLengthMask1   = 0x03 // meta[1] 0b00000011 (high bits)
	kPacketKeyLengthMask2   = 0xff // meta[2] 0b11111111 (medium bits)
	kPacketKeyLengthMask3   = 0xe0 // meta[3] 0b11100000 (low bits)
	kPacketValueLengthMask1 = 0x1f // meta[3] 0b00011111 (high bits)
	kPacketValueLengthMask2 = 0xff // meta[4] 0b11111111 (low bits)
)

var (
	// Shared table for efficient checksumming.
	kChecksumTable *crc64.Table
)

func init() {
	kChecksumTable = crc64.MakeTable(crc64.ECMA)
}

// Initializes the packet by setting capacity and zeroing all other metadata.
// Packet data itself is not zeroed. If the requested length is more than
// kPacketMaxByteLength, or if the resulting packet wouldn't be aligned,
// initialize() will panic.
func (p *packet) initialize(packetLength int) {
	if packetLength > kPacketMaxByteLength {
		panic(fmt.Errorf("%v overflows max packet length.", packetLength))
	}
	if packetLength%kPacketAlignment != 0 {
		panic(fmt.Errorf("Length of %v is not packet aligned.", packetLength))
	}
	p.crc = 0
	p.next = 0
	p.meta = [5]uint8{0, 0, 0, 0, 0}

	// Two lowest capacity bits are implicit from the required alignment.
	capacity := (packetLength - kPacketHeaderByteLength) >> 2
	p.meta[0] |= uint8(kPacketCapacityMask1 & (capacity >> 6))
	p.meta[1] |= uint8(kPacketCapacityMask2 & (capacity << 2))
}

// Total key and value storage capacity of the packet.
func (p *packet) capacity() int {
	capacity := int(p.meta[0]&kPacketCapacityMask1) << 6
	capacity |= int(p.meta[1]&kPacketCapacityMask2) >> 2
	return capacity<<2 + kPacketAlignmentAdjustment&0x3
}

// Remaining storage capacity of the packet.
func (p *packet) availableCapacity() int {
	return p.capacity() - p.keyLength() - p.valueLength()
}

// Total byte-length of the packet, including metadata header and capacity.
func (p *packet) packetLength() int {
	return kPacketHeaderByteLength + p.capacity()
}

// Byte-length of the key.
func (p *packet) keyLength() int {
	length := int(p.meta[1] & kPacketKeyLengthMask1 << 11)
	length |= int(p.meta[2] & kPacketKeyLengthMask2 << 3)
	length |= int(p.meta[3] & kPacketKeyLengthMask3 >> 5)
	return length
}

// Allocates capacity for storing the key, returning a mutable underlying
// slice. If a key or value is already set, or the requested key overflows
// packet capacity, setKey() will panic.
func (p *packet) setKey(length int) []byte {
	if p.keyLength() != 0 || p.valueLength() != 0 {
		panic(fmt.Errorf("%v is not empty.", p))
	}
	if length > p.availableCapacity() {
		panic(fmt.Errorf("Key length %v is too long for %v.", length, p))
	}
	// Note the key bits must be zero at this point.
	p.meta[1] |= uint8(kPacketKeyLengthMask1 & (length >> 11))
	p.meta[2] |= uint8(kPacketKeyLengthMask2 & (length >> 3))
	p.meta[3] |= uint8(kPacketKeyLengthMask3 & (length << 5))
	return p.data[:length]
}

// Returns the packet key. Do not mutate.
func (p *packet) key() []byte {
	return p.data[:p.keyLength()]
}

// Byte-length of the value.
func (p *packet) valueLength() int {
	length := int(p.meta[3] & kPacketValueLengthMask1 << 8)
	length |= int(p.meta[4])
	return length
}

// Allocates capacity for storing the value, returning a mutable underlying
// slice. setValue() may be called multiple times to overwrite an existing
// value, but will panic if the requested value would overflow packet capacity.
func (p *packet) setValue(valueLength int) []byte {
	keyLength := p.keyLength()
	if keyLength+valueLength > p.capacity() {
		panic(fmt.Errorf("Value length %v is too long for %v.", valueLength, p))
	}
	p.meta[3] = p.meta[3]&kPacketKeyLengthMask3 |
		uint8(kPacketValueLengthMask1&(valueLength>>8))
	p.meta[4] = uint8(kPacketValueLengthMask2 & valueLength)
	return p.data[keyLength : keyLength+valueLength]
}

// Returns the packet value. Do not mutate.
func (p *packet) value() []byte {
	keyLength := p.keyLength()
	return p.data[keyLength : keyLength+p.valueLength()]
}

// Whether this packet contains stale data, and may be reclaimed.
func (p *packet) isDead() bool {
	return p.meta[0]&kPacketDeadMask != 0
}

// Marks the packet as reclaimable.
func (p *packet) markDead() {
	p.meta[0] |= kPacketDeadMask
}

// Whether this packet is a continuation of a packet sequence.
func (p *packet) continuesSequence() bool {
	return p.meta[0]&kPacketContinuesMask != 0
}
func (p *packet) markContinuesSequence() {
	p.meta[0] |= kPacketContinuesMask
}

// Whether this packet is the completion of a packet sequence.
// Single-packet sequences should still set this bit.
func (p *packet) completesSequence() bool {
	return p.meta[0]&kPacketCompletesMask != 0
}
func (p *packet) markCompletesSequence() {
	p.meta[0] |= kPacketCompletesMask
}

// Asserts the combined metadata and content checksum is correct.
func (p *packet) checkIntegrity(contentSummer hash.Hash64) {
	invariant(p.keyLength()+p.valueLength() < p.capacity())
	invariant(p.computeCombinedChecksum(contentSummer) == p.crc)
}

// Computes the running checksum of this packet's key and value content.
// contentSummer is expected to be a running checksum of all antecedent
// packet content. As packets always arrange key content first, followed by
// value content, any arrangement of specific packet sequences will produce
// identical completed checksum.
func (p *packet) computeContentChecksum(contentSummer hash.Hash64) uint64 {
	contentSummer.Write(p.key())
	contentSummer.Write(p.value())
	return contentSummer.Sum64()
}

// Computes the checksum of this packet's meta-data. Specifically:
// packet.next, isDead(), continuesSequence(), and completesSequence().
func (p *packet) computeMetaChecksum() uint64 {
	var buf [kPacketHeaderByteLength]byte
	buf[0] = byte(p.next)
	buf[1] = byte(p.next >> 8)
	buf[2] = byte(p.next >> 16)
	buf[3] = byte(p.next >> 24)
	for i, v := range p.meta {
		buf[i+4] = byte(v)
	}
	metaSummer := crc64.New(kChecksumTable)
	metaSummer.Write(buf[:])
	return metaSummer.Sum64()
}

// Efficient updates of the packet's checksum to reflect changes to metadata.
func (p *packet) updateMetaOfCombinedChecksum(oldMetaChecksum uint64) {
	p.crc ^= oldMetaChecksum
	p.crc ^= p.computeMetaChecksum()
}

// Combined (xor) checksum of the running content sum, plus meta-data.
func (p *packet) computeCombinedChecksum(contentSummer hash.Hash64) uint64 {
	return p.computeMetaChecksum() ^ p.computeContentChecksum(contentSummer)
}

func (p *packet) String() string {
	fields := append([]string{},
		fmt.Sprint("crc = ", p.crc),
		fmt.Sprint("next = ", p.next),
		fmt.Sprint("capacity = ", p.capacity()),
		fmt.Sprint("available = ", p.availableCapacity()),
		fmt.Sprint("keyLength = ", p.keyLength()),
		fmt.Sprint("valueLength = ", p.valueLength()),
		fmt.Sprint("packetLength = ", p.packetLength()))
	if p.isDead() {
		fields = append(fields, "dead")
	}
	if p.continuesSequence() {
		fields = append(fields, "continues")
	}
	if p.completesSequence() {
		fields = append(fields, "completes")
	}
	key, value := p.key(), p.value()
	if len(key) > 10 {
		key = key[:10]
	}
	if len(value) > 10 {
		value = value[:10]
	}
	fields = append(fields,
		fmt.Sprint("key = ", key),
		fmt.Sprint("value = ", value))
	return fmt.Sprintf("packet{%v}", strings.Join(fields, ", "))
}
