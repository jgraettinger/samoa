

namespace samoa {
namespace persistence {
namespace rolling_hash {


class hash_ring
{
public:

    typedef hash_ring_packet packet_t;
    typedef hash_ring_element element_t;

    /// packet bulkheads every 1 << 19 = 524288 bytes
    const uint32_t bulkhead_shift = 19;

    struct locator
    {
        uint32_t index_location;

        packet * previous_chained_head;
        packet * element_head;
    };

    hash_ring(void * region_ptr, uint32_t region_size, uint32_t index_size);

    virtual ~hash_ring();


    locator locate_key(const std::string &) const;


    locator allocate_packets(uint32_t capacity) const;


    void reclaim_head();

    void rotate_head();


    packet_t * head() const;

    packet_t * next_packet(const packet_t *) const;

    uint32_t index_offset() const;

    uint32_t packet_offset() const;

    uint32_t total_region_size() const;

    uint32_t used_region_size() const;

    uint32_t total_index_size() const;

    uint32_t used_index_size() const;

    uint32_t ring_begin_offset() const;

    uint32_t ring_end_offset() const;

protected:

    uint8_t * _region_ptr;
    uint32_t _region_size;

    enum persistence_state_enum {
        FROZEN = 0xf0f0f0f0,
        ACTIVE = FROZEN + 1
    };

    struct table_header {

        uint32_t persistence_state;
        uint32_t total_record_count;
        uint32_t live_record_count;

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

#include "samoa/persistence/rolling_hash.impl.hpp"

#endif

