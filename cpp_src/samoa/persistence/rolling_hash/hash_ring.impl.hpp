
#include "samoa/persistence/rolling_hash/packet.hpp"
#include "samoa/persistence/rolling_hash/error.hpp"
#include <boost/functional/hash.hpp>
#include <ext/algorithm>

namespace samoa {
namespace persistence {
namespace rolling_hash {

template<typename KeyIterator>
hash_ring::locator hash_ring::locate_key(
    const KeyIterator & key_begin, const KeyIterator & key_end) const
{
    locator loc = {0, nullptr, nullptr};

    size_t hash_value = boost::hash_range(key_begin, key_end);
    loc.index_location = hash_value % _index_size;

    // dereference index location to head packet offset
    uint32_t next_offset = *reinterpret_cast<uint32_t*>(
        _region_ptr + index_region_offset() + \
            loc.index_location * sizeof(uint32_t));

    while(next_offset != 0)
    {
        RING_INTEGRITY_CHECK(_region_size >= \
            next_offset + packet::min_packet_byte_length());

        // follow chain to next packet
        loc.previous_chained_head = loc.element_head;
        loc.element_head = reinterpret_cast<packet*>(
            _region_ptr + next_offset);

        element elem(this, loc.element_head);

        if(!__gnu_cxx::lexicographical_compare_3way(
            key_begin, key_end,
            key_gather_iterator(elem), key_gather_iterator()))
        {
            // keys match
            return loc;
        }
        next_offset = loc.element_head->hash_chain_next();
    }

    // key not found in chain
    loc.previous_chained_head = loc.element_head;
    loc.element_head = nullptr;

    return loc;
}

}
}
}

