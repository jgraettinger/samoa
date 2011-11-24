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
        memset(region, 0, sizeof(hash_ring::table_header));

        return std::unique_ptr<heap_hash_ring>(
            new heap_hash_ring(region, region_size, index_size));
    }

    virtual ~heap_hash_ring()
    { delete [] _region_ptr; }

private:

    heap_hash_ring(uint8_t * region, size_t region_size, size_t index_size)
     : hash_ring::hash_ring(region, region_size, index_size)
    { }

};

}
}
}

#endif

