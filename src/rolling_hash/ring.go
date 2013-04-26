package rollingHash

import (
	"fmt"
	"reflect"
	"unsafe"
)

type initializationState int

const (
	kNotInitialized initializationState = 0
	kInitialized    initializationState = 1
)

type tableHeader struct {
	// Serialization state of the table.
	initializationState initializationState
	// First offset 
	begin int
	// First offset beyond the last packet.
	end int
	// Whether the written portion of the ring wraps around the end of the
	// region. Under this condition, end will be less then or equal to begin.
	wrapped bool
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

// Panics if region is too small for indexSize.
func (ring *Ring) initialize(region []byte, indexSize int) error {
	minSize := kRingHeaderByteLength + indexSize*kUint32ByteLength
	if len(region) < minSize {
		return fmt.Errorf("Region size %v is insufficient for indexSize %v",
			len(region), indexSize)
	}
	ring.region = region

	// Break the type system to re-interpret (as tableHeader)
	// the beginning of the region.
	ring.header = (*tableHeader)(unsafe.Pointer(&region[0]))

	// Break the type system to re-interpret (as []uint32) the
	// portion of region containing the index.
	indexHeader := *(*reflect.SliceHeader)(
		unsafe.Pointer(&region[kRingHeaderByteLength]))
	indexHeader.Len = indexSize
	indexHeader.Cap = indexSize
	ring.index = *(*[]uint32)(unsafe.Pointer(&indexHeader))

	if ring.header.initializationState == kNotInitialized {
		ring.header.begin = ring.storageOffset()
		ring.header.end = ring.storageOffset()
		ring.header.initializationState = kInitialized
	}
	return nil
}

func (ring *Ring) head() *packet {
	if !ring.header.wrapped && ring.header.begin == ring.header.end {
		return nil
	}
	return (*packet)(unsafe.Pointer(&ring.region[ring.header.begin]))
}

func (ring *Ring) nextPacket(pkt *packet) *packet {
	offset := ring.packetOffset(pkt) + pkt.packetLength()
	if ring.header.wrapped && offset == len(ring.region) {
		// Wrap around to the beginning of the ring.
		offset = ring.storageOffset()
	}
	if offset == ring.header.end {
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
	return kRingHeaderByteLength + kUint32ByteLength*len(ring.index)
}
func (ring *Ring) storageSize() int {
	return len(ring.region) - ring.storageOffset()
}
func (ring *Ring) storageUsed() int {
	if ring.header.wrapped {
		return len(ring.region) + ring.header.end -
			ring.header.begin - ring.storageOffset()
	}
	return ring.header.end - ring.header.begin
}

func (ring *Ring) allocatePackets(capacity int) (head *packet) {
	begin := ring.header.begin
	end := ring.header.end
	wrapped := ring.header.wrapped

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

		if blockLength < kPacketMinByteLength {
			panic(blockLength)
		}
		if blockLength%kPacketAlignment != 0 {
			panic(blockLength)
		}

		// Start with length required to fit remaining capacity, rounded up for alignment.
		packetLength := kPacketHeaderByteLength + capacity
		if packetLength%kPacketAlignment != 0 {
			packetLength += kPacketAlignment - (packetLength % kPacketAlignment)
		}
		// Bound by the maximum packet length, and by the block length.
		if packetLength > kPacketMaxByteLength {
			packetLength = kPacketMaxByteLength
		}
		if packetLength > blockLength {
			packetLength = blockLength
		}
		// Ensure we'll leave a valid block remainder after this allocation.
		remainder := blockLength - packetLength
		if remainder != 0 && remainder < kPacketMinByteLength {
			if packetLength+remainder > kPacketMaxByteLength {
				// Shorten to ensure a minimum length packet may be written.
				packetLength -= kPacketMinByteLength - remainder
			} else {
				// Elongate packet to consume the remainder.
				packetLength += remainder
			}
		}

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
	}
	ring.header.end = end
	ring.header.wrapped = wrapped
	return
}

func (ring *Ring) reclaimHead() int {
	reclaimedBytes := 0

	pkt := ring.head()
	firstInSequence := true

	for {
		if pkt == nil {
			panic(nil)
		}
		if !pkt.isDead() {
			panic(pkt)
		}
		if firstInSequence {
			if pkt.continuesSequence() {
				panic(pkt)
			}
			firstInSequence = false
		} else if !pkt.continuesSequence() {
			panic(pkt)
		}

		ring.header.begin += pkt.packetLength()
		if ring.header.begin > len(ring.region) {
			panic(pkt)
		}
		if ring.header.wrapped && ring.header.begin == len(ring.region) {
			ring.header.begin = ring.storageOffset()
			ring.header.wrapped = false
		}
		reclaimedBytes += pkt.packetLength()

		if pkt.completesSequence() {
			break
		}
		pkt = ring.head()
	}
	return reclaimedBytes
}
