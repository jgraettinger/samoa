package rollingHash

import (
	"fmt"
	"log"
	"reflect"
	"runtime/debug"
	"strings"
	"unsafe"
)

type initializationState int64

const (
	kNotInitialized initializationState = 0
	kInitialized    initializationState = 1
)

type tableHeader struct {
	// Serialization state of the table.
	initializationState initializationState
	// First offset.
	begin int64
	// First offset beyond the last packet.
	end int64
	// Non-zero iff the written portion of the ring wraps around the end of the
	// region. Under this condition, end will be less then or equal to begin.
	wrapped int64
}

type Ring struct {
	region []byte
	header *tableHeader
	index  []uint32
}

const (
	kRingHeaderByteLength = int(unsafe.Sizeof(tableHeader{}))
	kRingBulkheadShift    = 20
	kUint32ByteLength     = int(unsafe.Sizeof(uint32(0)))
)

func invariant(value bool, context ...interface{}) {
	if !value {
		panic(fmt.Errorf("Invariant failure.\nContext:%v\nStack:\n%v",
			context, string(debug.Stack())))
	}
}

func (ring *Ring) begin() int {
	return int(ring.header.begin)
}
func (ring *Ring) setBegin(begin int) {
	ring.header.begin = int64(begin)
}

func (ring *Ring) end() int {
	return int(ring.header.end)
}
func (ring *Ring) setEnd(end int) {
	ring.header.end = int64(end)
}

func (ring *Ring) wrapped() bool {
	return ring.header.wrapped != 0
}
func (ring *Ring) setWrapped(wrapped bool) {
	if wrapped {
		ring.header.wrapped = 1
	} else {
		ring.header.wrapped = 0
	}
}

// Panics if region is too small for indexSize.
func (ring *Ring) initialize(region []byte, indexSize int) {
	invariant(len(region) > kRingHeaderByteLength+indexSize*kUint32ByteLength)

	ring.region = region
	// Break the type system to re-interpret (as tableHeader)
	// the beginning of the region.
	ring.header = (*tableHeader)(unsafe.Pointer(&region[0]))

	// Break the type system to assign the appropriate underlying
	// storage region to the ring.index []uint32 slice.
	indexHeader := (*reflect.SliceHeader)(unsafe.Pointer(&ring.index))
	indexHeader.Data = uintptr(unsafe.Pointer(&region[kRingHeaderByteLength]))
	indexHeader.Len = indexSize
	indexHeader.Cap = indexSize

	if ring.header.initializationState == kNotInitialized {
		ring.header.begin = int64(ring.storageOffset())
		ring.header.end = int64(ring.storageOffset())
		ring.header.initializationState = kInitialized
	}
}

func (ring *Ring) head() *packet {
	if !ring.wrapped() && ring.header.begin == ring.header.end {
		return nil
	}
	return (*packet)(unsafe.Pointer(&ring.region[ring.header.begin]))
}

func (ring *Ring) nextPacket(pkt *packet) *packet {
	offset := ring.packetOffset(pkt) + pkt.packetLength()
	if ring.wrapped() && offset == len(ring.region) {
		// Wrap around to the beginning of the ring.
		offset = ring.storageOffset()
	}
	if int64(offset) == ring.header.end {
		// Reached the ring end.
		return nil
	}
	return (*packet)(unsafe.Pointer(&ring.region[offset]))
}

func (ring *Ring) packetOffset(pkt *packet) int {
	return int(uintptr(unsafe.Pointer(pkt)) -
		uintptr(unsafe.Pointer(&ring.region[0])))
}

func (ring *Ring) storageOffset() int {
	offset := kRingHeaderByteLength + kUint32ByteLength*len(ring.index)
	if offset%kPacketAlignment != 0 {
		offset += kPacketAlignment - (offset % kPacketAlignment)
	}
	return offset
}
func (ring *Ring) storageSize() int {
	return len(ring.region) - ring.storageOffset()
}
func (ring *Ring) storageUsed() int {
	if ring.wrapped() {
		return len(ring.region) + ring.end() -
			ring.begin() - ring.storageOffset()
	}
	return ring.end() - ring.begin()
}

func (ring *Ring) allocatePackets(capacity int) (head *packet) {
	begin := ring.begin()
	end := ring.end()
	wrapped := ring.wrapped()

	for capacity != 0 {
		// Determine the appropriate boundary for the next packet.
		// This will be either the region extent, the ring beginning,
		// or (common case) the next bulkhead boundary.
		nextBoundary := len(ring.region)
		if wrapped {
			if end+kPacketHeaderByteLength+capacity > begin {
				// Can't be met. Would require allocating over the ring head.
				head = nil
				return
			}
			nextBoundary = begin
		}
		if end>>kRingBulkheadShift != nextBoundary>>kRingBulkheadShift {
			// Packets musn't overlap bulkhead boundaries.
			bulkhead := end >> kRingBulkheadShift
			nextBoundary = (bulkhead + 1) << kRingBulkheadShift
		}
		blockLength := nextBoundary - end
		log.Print("nextBoundary: ", nextBoundary)
		log.Print("blockLength: ", blockLength)

		invariant(blockLength >= kPacketMinByteLength)
		invariant(blockLength%kPacketAlignment == 0)

		// Start with length required to fit remaining capacity, rounded up for alignment.
		packetLength := kPacketHeaderByteLength + capacity
		log.Print("initial packetLength: ", packetLength)
		if packetLength%kPacketAlignment != 0 {
			packetLength += kPacketAlignment - (packetLength % kPacketAlignment)
			log.Print("adjusted for alignment: ", packetLength)
		}
		// Bound by the maximum packet length, and by the block length.
		if packetLength > kPacketMaxByteLength {
			packetLength = kPacketMaxByteLength
			log.Print("bounded by max packet length: ", packetLength)
		}
		if packetLength > blockLength {
			packetLength = blockLength
			log.Print("bounded by block length: ", packetLength)
		}
		// Ensure we'll leave a valid block remainder after this allocation.
		remainder := blockLength - packetLength
		log.Print("remainder: ", remainder)
		if remainder != 0 && remainder < kPacketMinByteLength {
			if packetLength+remainder > kPacketMaxByteLength {
				// Shorten to ensure a minimum length packet may be written.
				packetLength -= kPacketMinByteLength - remainder
				log.Print("shortened to ensure min length packet: ", packetLength)
			} else {
				// Elongate packet to consume the remainder.
				packetLength += remainder
				log.Print("elongated to ensure min length packet: ", packetLength)
			}
		}

		log.Print("allocating at offset: ", end)
		log.Print("pointer: ", unsafe.Pointer(&ring.region[end]))
		pkt := (*packet)(unsafe.Pointer(&ring.region[end]))
		pkt.initialize(packetLength)

		if head == nil {
			head = pkt
		} else {
			pkt.markContinuesSequence()
		}

		if pkt.capacity() >= capacity {
			pkt.markCompletesSequence()
			capacity = 0
		} else {
			capacity -= pkt.capacity()
		}

		if end+packetLength == len(ring.region) {
			// We wrapped around the end of the ring.
			wrapped = true
			end = ring.storageOffset()
		} else {
			end += packetLength
		}
		log.Print("allocated packet ", pkt)
		log.Print("remaining capacity: ", capacity)
	}
	ring.setEnd(end)
	ring.setWrapped(wrapped)
	return
}

func (ring *Ring) reclaimHead() int {
	reclaimedBytes := 0

	pkt := ring.head()
	firstInSequence := true

	for {
		invariant(pkt != nil)
		invariant(pkt.isDead())

		if firstInSequence {
			invariant(!pkt.continuesSequence())
			firstInSequence = false
		} else {
			invariant(pkt.continuesSequence())
		}

		ring.setBegin(ring.begin() + pkt.packetLength())
		invariant(ring.begin() <= len(ring.region))

		if ring.wrapped() && ring.begin() == len(ring.region) {
			ring.setBegin(ring.storageOffset())
			ring.setWrapped(false)
		}
		reclaimedBytes += pkt.packetLength()

		if pkt.completesSequence() {
			break
		}
		pkt = ring.head()
	}
	return reclaimedBytes
}

func (ring *Ring) String() string {
	fields := append([]string{},
		fmt.Sprint("region = ", unsafe.Pointer(&ring.region[0])),
		fmt.Sprint("regionLength = ", len(ring.region)),
		fmt.Sprint("indexLength = ", len(ring.index)),
		fmt.Sprint("storageOffset = ", ring.storageOffset()),
		fmt.Sprint("storageSize = ", ring.storageSize()),
		fmt.Sprint("storageUsed = ", ring.storageUsed()),
		fmt.Sprint("initializationState = ", ring.header.initializationState),
		fmt.Sprint("begin = ", ring.begin()),
		fmt.Sprint("end = ", ring.end()),
		fmt.Sprint("wrapped = ", ring.wrapped()),
		fmt.Sprint("head = ", ring.head()))
	return fmt.Sprintf("ring{%v}", strings.Join(fields, ", "))
}
