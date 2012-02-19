
#include "samoa/persistence/rolling_hash/mapped_hash_ring.hpp"
#include "samoa/persistence/rolling_hash/error.hpp"
#include "samoa/core/memory_map.hpp"
#include "samoa/log.hpp"
#include <fstream>

namespace samoa {
namespace persistence {
namespace rolling_hash {

mapped_hash_ring::mapped_hash_ring(core::memory_map_ptr_t && mmp,
    uint32_t region_size, uint32_t index_size)
 :  hash_ring::hash_ring(
        reinterpret_cast<uint8_t*>(mmp->get_region_address()),
        region_size, index_size),
   _memory_map(std::move(mmp))
{
    if(_memory_map->was_resized())
    {
        // zero-initialize the table header & index
        memset(_region_ptr, 0,
            sizeof(table_header) + sizeof(uint32_t) * _index_size);

        _tbl.begin = ring_region_offset();
        _tbl.end = ring_region_offset();
    }
    else
        RING_INTEGRITY_CHECK(_tbl.persistence_state == FROZEN);

    // set as active, and flush to disk
    _tbl.persistence_state = ACTIVE;
    _memory_map->get_region()->flush();
}

mapped_hash_ring::~mapped_hash_ring()
{
    _tbl.persistence_state = FROZEN;
}

std::unique_ptr<mapped_hash_ring> mapped_hash_ring::open(
    const std::string & file, uint32_t region_size, uint32_t index_size)
{
    core::memory_map_ptr_t mmp(new core::memory_map(file, region_size));

    std::unique_ptr<mapped_hash_ring> mhr(
        new mapped_hash_ring(std::move(mmp), region_size, index_size));

    return mhr;
}

}
}
}

