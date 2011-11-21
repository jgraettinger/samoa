#ifndef SAMOA_PERSISTENCE_ROLLING_HASH_HASH_RING_HPP
#define SAMOA_PERSISTENCE_ROLLING_HASH_HASH_RING_HPP

#include "samoa/persistence/rolling_hash/fwd.hpp"
#include <string>
#include <cstdint>

namespace samoa {
namespace persistence {
namespace rolling_hash {

class hash_ring
{
public:

    /// packet bulkheads every 1 << 19 = 524288 bytes
    static const uint32_t bulkhead_shift = 19;

    struct locator
    {
        uint32_t index_location;

        packet * previous_chained_head;
        packet * element_head;
    };

    hash_ring(uint8_t * region_ptr, uint32_t region_size, uint32_t index_size);

    virtual ~hash_ring();

    template<typename KeyIterator>
    locator locate_key(const KeyIterator & begin, const KeyIterator & end) const;

    locator locate_key(const std::string &) const;

    packet * allocate_packets(uint32_t capacity);

    void reclaim_head();

    void rotate_head();

    void update_hash_chain(const locator & loc, uint32_t new_offset);

    packet * head() const;

    packet * next_packet(const packet *) const;

    uint32_t packet_offset(const packet *) const;

    uint32_t region_size() const
    { return _region_size; }

    uint32_t index_size() const
    { return _index_size; }

    uint32_t index_region_offset() const
    { return sizeof(table_header); }

    uint32_t ring_region_offset() const
    { return index_region_offset() + index_size() * sizeof(uint32_t); }

    uint32_t begin_offset() const
    { return _tbl.begin; }

    uint32_t end_offset() const
    { return _tbl.end; }

    bool is_wrapped() const
    { return _tbl.is_wrapped; }

protected:

    uint8_t * _region_ptr;
    uint32_t _region_size;
    uint32_t _index_size;

    enum persistence_state_enum {
        NEW = 0,
        FROZEN = 0xf0f0f0f0,
        ACTIVE = FROZEN + 1
    };

    struct table_header {

        uint32_t persistence_state;

        // offset of first packet
        uint32_t begin;
        // 1 beyond last packet
        uint32_t end;

        // whether the written portion of the ring wraps
        //  around the end of the region (the only condition
        //  under which end may be less than begin)
        bool is_wrapped;
    };

    table_header & _tbl;
};

}
}
}

#endif

