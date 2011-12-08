#ifndef SAMOA_PERSISTENCE_ROLLING_HASH_HEAP_HASH_RING_HPP
#define SAMOA_PERSISTENCE_ROLLING_HASH_HEAP_HASH_RING_HPP

#include "samoa/persistence/rolling_hash/hash_ring.hpp"

namespace samoa {
namespace persistence {
namespace rolling_hash {

class heap_hash_ring : public hash_ring
{
public:

    static std::unique_ptr<heap_hash_ring> open(
        size_t region_size, size_t index_size)
    {
        uint8_t * region = new uint8_t[region_size];

        return std::unique_ptr<heap_hash_ring>(
            new heap_hash_ring(region, region_size, index_size));
    }

    virtual ~heap_hash_ring()
    { delete [] _region_ptr; }

private:

    heap_hash_ring(uint8_t * region, size_t region_size, size_t index_size)
     : hash_ring::hash_ring(region, region_size, index_size)
    {
        // zero-initialize the table header & index
        memset(_region_ptr, 0,
            sizeof(table_header) + sizeof(uint32_t) * _index_size);

        _tbl.begin = ring_region_offset();
        _tbl.end = ring_region_offset();
    }

};

}
}
}

#endif

