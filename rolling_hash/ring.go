package rollingHash

import (
)

type tableHeader struct {
	// First offset 
	begin, end uint64
	// First offset beyond the last packet.
	uint64 end

	// Whether the written portion of the ring wraps around the end of the
	// region. Under this condition, end will be less then or equal to begin.
	wrapped bool
}





type Ring struct {


	region []byte
}
