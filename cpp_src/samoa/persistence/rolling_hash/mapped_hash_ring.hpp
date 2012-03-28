#ifndef SAMOA_PERSISTENCE_ROLLING_HASH_MAPPED_HASH_RING_HPP
#define SAMOA_PERSISTENCE_ROLLING_HASH_MAPPED_HASH_RING_HPP

#include "samoa/persistence/rolling_hash/hash_ring.hpp"
#include "samoa/core/fwd.hpp"
#include <memory>

namespace samoa {
namespace persistence {
namespace rolling_hash {

class mapped_hash_ring : public hash_ring
{
public:

    static std::unique_ptr<mapped_hash_ring> open(
        const std::string & file,
        uint32_t region_size,
        uint32_t index_size);

    virtual ~mapped_hash_ring();

private:

    mapped_hash_ring(core::memory_map_ptr_t,
        uint32_t region_size,
        uint32_t index_size,
        bool is_new);

    core::memory_map_ptr_t _memory_map;
};

}
}
}

#endif

