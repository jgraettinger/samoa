package rollingHash

import (
	"fmt"
	"strings"
	"unsafe"
)

type packet struct {
	checksum uint32
	next     uint32
	meta     [5]uint8
	data     [1 << 13]byte
}

const (
	// Placement of packets within a ring must adhere to the packet
	// byte-alignment (to do otherwise would result in a bus error).
	// Alignment will be 4 bytes on most architectures.
	packetAlignment = uint32(unsafe.Alignof(packet{}))
	// Byte-length of the packet metadata header.
	headerByteLength = uint32(unsafe.Offsetof(packet{}.data))
	// Byte-alignment of the packet header.
	headerAlignment = headerByteLength % packetAlignment
	// Byte-alignment of packet capacity required to correct for
	// the header alignment, ensuring the total packet length
	// obeys the packet alignment.
	capacityAlignmentAdjustment = packetAlignment - headerAlignment
	// The capacity field is 11 bits, with 2 implicit lower bits due to alignment.
	maxCapacity = 1<<13 - headerAlignment

	minPacketByteLength = headerByteLength + capacityAlignmentAdjustment
	maxPacketByteLength = headerByteLength + maxCapacity

	deadMask         = 0x80 // meta[0] 0b10000000
	continuesMask    = 0x40 // meta[0] 0b01000000
	completesMask    = 0x20 // meta[0] 0b00100000
	capacityMask1    = 0x1f // meta[0] 0b00011111 (high bits)
	capacityMask2    = 0xfc // meta[1] 0b11111100 (low bits)
	keyLengthMask1   = 0x03 // meta[1] 0b00000011 (high bits)
	keyLengthMask2   = 0xff // meta[2] 0b11111111 (medium bits)
	keyLengthMask3   = 0xe0 // meta[3] 0b11100000 (low bits)
	valueLengthMask1 = 0x1f // meta[3] 0b00011111 (high bits)
	valueLengthMask2 = 0xff // meta[4] 0b11111111 (low bits)
)

// Initializes the packet by setting capacity and zeroing all other metadata.
// Packet data itself is not zeroed. If the requested capacity is more than
// maxCapacity, or if the resulting packet wouldn't be aligned,
// initialize() will panic.
func (p *packet) initialize(capacity uint32) {
	if capacity > maxCapacity {
		panic(fmt.Errorf("Capacity of %v is larger than max capacity %v.",
			capacity, maxCapacity))
	}
	if capacity%packetAlignment != capacityAlignmentAdjustment {
		panic(fmt.Errorf("Capacity of %v is not %v-aligned.",
			capacity, packetAlignment))
	}
	p.checksum = 0
	p.next = 0
	p.meta = [5]uint8{0, 0, 0, 0, 0}

	// Two lowest bits are implicit from the required capacity alignment.
	capacity = capacity >> 2
	p.meta[0] |= uint8(capacityMask1 & (capacity >> 6))
	p.meta[1] |= uint8(capacityMask2 & (capacity << 2))
}

// Total key and value storage capacity of the packet.
func (p *packet) capacity() uint32 {
	capacity := uint32(p.meta[0]&capacityMask1) << 6
	capacity |= uint32(p.meta[1]&capacityMask2) >> 2
	return capacity<<2 + capacityAlignmentAdjustment&0x3
}

// Remaining storage capacity of the packet.
func (p *packet) availableCapacity() uint32 {
	return p.capacity() - p.keyLength() - p.valueLength()
}

// Total byte-length of the packet, including metadata header and capacity.
func (p *packet) packetLength() uint32 {
	return headerByteLength + p.capacity()
}

// Byte-length of the key.
func (p *packet) keyLength() uint32 {
	length := uint32(p.meta[1] & keyLengthMask1 << 11)
	length |= uint32(p.meta[2] & keyLengthMask2 << 3)
	length |= uint32(p.meta[3] & keyLengthMask3 >> 5)
	return length
}

// Allocates capacity for storing the key, returning a mutable underlying
// slice. If a key or value is already set, or the requested key overflows
// packet capacity, setKey() will panic.
func (p *packet) setKey(length uint32) []byte {
	if p.keyLength() != 0 || p.valueLength() != 0 {
		panic(fmt.Errorf("%v is not empty.", p))
	}
	if length > p.availableCapacity() {
		panic(fmt.Errorf("Key length %v is too long for %v.", length, p))
	}
	// Note the key bits must be zero at this point.
	p.meta[1] |= uint8(keyLengthMask1 & (length >> 11))
	p.meta[2] |= uint8(keyLengthMask2 & (length >> 3))
	p.meta[3] |= uint8(keyLengthMask3 & (length << 5))
	return p.data[:length]
}

// Returns the packet key. Do not mutate.
func (p *packet) key() []byte {
	return p.data[:p.keyLength()]
}

// Byte-length of the value.
func (p *packet) valueLength() uint32 {
	length := uint32(p.meta[3] & valueLengthMask1 << 8)
	length |= uint32(p.meta[4])
	return length
}

// Allocates capacity for storing the value, returning a mutable underlying
// slice. setValue() may be called multiple times to overwrite an existing
// value, but will panic if the requested value would overflow packet capacity.
func (p *packet) setValue(valueLength uint32) []byte {
	keyLength := p.keyLength()
	if keyLength+valueLength > p.capacity() {
		panic(fmt.Errorf("Value length %v is too long for %v.", valueLength, p))
	}
	p.meta[3] = p.meta[3]&keyLengthMask3 |
		uint8(valueLengthMask1&(valueLength>>8))
	p.meta[4] = uint8(valueLengthMask2 & valueLength)
	return p.data[keyLength : keyLength+valueLength]
}

// Returns the packet value. Do not mutate.
func (p *packet) value() []byte {
	keyLength := p.keyLength()
	return p.data[keyLength : keyLength+p.valueLength()]
}

// Whether this packet contains stale data, and may be reclaimed.
func (p *packet) isDead() bool {
	return p.meta[0]&deadMask != 0
}

// Marks the packet as reclaimable.
func (p *packet) markDead() {
	p.meta[0] |= deadMask
}

// Whether this packet is a continuation of a packet sequence.
func (p *packet) continuesSequence() bool {
	return p.meta[0]&continuesMask != 0
}

func (p *packet) markContinuesSequence() {
	p.meta[0] |= continuesMask
}

// Whether this packet is the completion of a packet sequence.
// Single-packet sequences should still set this bit.
func (p *packet) completesSequence() bool {
	return p.meta[0]&completesMask != 0
}
func (p *packet) markCompletesSequence() {
	p.meta[0] |= completesMask
}

func (p *packet) String() string {
	fields := append([]string{},
		fmt.Sprint("checksum = ", p.checksum),
		fmt.Sprint("next = ", p.next),
		fmt.Sprint("capacity = ", p.capacity()),
		fmt.Sprint("available = ", p.availableCapacity()),
		fmt.Sprint("keyLength = ", p.keyLength()),
		fmt.Sprint("valueLength = ", p.valueLength()))
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
