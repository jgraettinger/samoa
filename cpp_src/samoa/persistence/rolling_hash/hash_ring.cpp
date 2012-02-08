
#include "samoa/persistence/rolling_hash/hash_ring.hpp"
#include "samoa/persistence/rolling_hash/key_gather_iterator.hpp"
#include "samoa/persistence/rolling_hash/value_gather_iterator.hpp"
#include "samoa/core/buffer_region.hpp"
#include "samoa/error.hpp"
#include "samoa/log.hpp"

namespace samoa {
namespace persistence {
namespace rolling_hash {


hash_ring::hash_ring(uint8_t * region_ptr, 
    uint32_t region_size, uint32_t index_size)
 :  _region_ptr(region_ptr),
    _region_size(region_size),
    _index_size(index_size),
    _tbl(*reinterpret_cast<table_header*>(region_ptr))
{
    uint32_t expected_size = sizeof(table_header);
    expected_size = index_size * sizeof(uint32_t);

    SAMOA_ASSERT(region_size > expected_size);

    // additional initialization & corruption checks
    //  are expected to be done by subclasses
}

hash_ring::~hash_ring()
{ }

hash_ring::locator hash_ring::locate_key(const std::string & key) const
{
    return locate_key(std::begin(key), std::end(key));
}

packet * hash_ring::allocate_packets(uint32_t capacity)
{
    packet * head = 0;

    // 64-bit type to avoid integer overflow
    uint64_t cur_end = _tbl.end;
    bool cur_is_wrapped = _tbl.is_wrapped;

    bool first_in_sequence = true;

    while(capacity)
    {
        uint32_t next_boundary = _region_size;

        if(cur_is_wrapped)
        {
            if(cur_end + packet::header_length() + capacity > _tbl.begin)
            {
                // would overwrite ring head
                return nullptr;
            }
            next_boundary = _tbl.begin;
        }

        if((cur_end >> bulkhead_shift) != \
            (next_boundary >> bulkhead_shift))
        {
            // packets musn't overlap bulkhead boundaries
            uint32_t bulkhead = (cur_end >> bulkhead_shift);
            next_boundary = (bulkhead + 1) << bulkhead_shift;
        }

        uint32_t block_length = next_boundary - cur_end;

        // we should have left ourselves at least this much room
        SAMOA_ASSERT(block_length >= packet::min_packet_byte_length());
        // TODO debug invariant
        SAMOA_ASSERT(!(block_length % sizeof(uint32_t)));

        uint32_t packet_length = std::min(block_length,
            packet::header_length() + std::min(capacity, packet::max_capacity()));

        // round up to uint32_t-aligned length
        //   this may only happen if capacity bounds packet-length
        if(packet_length % sizeof(uint32_t))
        {
            packet_length += sizeof(uint32_t) - (packet_length % sizeof(uint32_t));
        }

        uint32_t remainder = block_length - packet_length;
        if(remainder && remainder < packet::min_packet_byte_length())
        {
            if(packet_length + remainder > packet::max_packet_byte_length())
            {
                // shorten packet, so that a minimum length packet may be written
                packet_length -= packet::min_packet_byte_length() - remainder;
            }
            else
            {
                // elongate packet to consume remainder
                packet_length += remainder;
            }
        }

        //LOG_INFO("creating packet length " << packet_length << " capacity " << capacity);

        // initialize packet
        packet * pkt = reinterpret_cast<packet*>(_region_ptr + cur_end);
        new (pkt) packet(packet_length - packet::header_length());

        if(first_in_sequence)
        {
            first_in_sequence = false;
            head = pkt;
        }
        else
            pkt->set_continues_sequence();

        if(pkt->capacity() >= capacity)
        {
            pkt->set_completes_sequence();
            capacity = 0;
        }
        else
            capacity -= pkt->capacity();

        if(cur_end + packet_length == _region_size)
        {
            // we wrapped around the ring end
            cur_is_wrapped = true;
            cur_end = ring_region_offset();
        }
        else
            cur_end += packet_length;
    }
    _tbl.is_wrapped = cur_is_wrapped;
    _tbl.end = cur_end;

    return head;
}

uint32_t hash_ring::reclaim_head()
{
    uint32_t reclaimed_bytes = 0;

    packet * pkt = head();
    bool first_in_sequence = true;

    while(true)
    {
        SAMOA_ASSERT(pkt && pkt->is_dead());

        if(first_in_sequence)
        {
            RING_INTEGRITY_CHECK(!pkt->continues_sequence());
            first_in_sequence = false;
        }
        else
            RING_INTEGRITY_CHECK(pkt->continues_sequence());

        _tbl.begin += pkt->packet_length();
        if(_tbl.is_wrapped && _tbl.begin == _region_size)
        {
            _tbl.begin = ring_region_offset();
            _tbl.is_wrapped = false;
        }
        RING_INTEGRITY_CHECK(_tbl.begin < _region_size);

        reclaimed_bytes += pkt->packet_length();

        if(pkt->completes_sequence())
            break;

        pkt = head();
    }
    return reclaimed_bytes;
}

void hash_ring::update_hash_chain(const locator & loc,
    uint32_t new_offset)
{
    SAMOA_ASSERT(new_offset >= ring_region_offset() && \
        new_offset < _region_size);

    if(loc.previous_chained_head)
    {
    	uint32_t meta_cs = loc.previous_chained_head->compute_meta_checksum();
    	// update previous element in the hash chain
        loc.previous_chained_head->set_hash_chain_next(new_offset);
        loc.previous_chained_head->update_meta_of_combined_checksum(meta_cs);
    }
    else
    {
        // offset is stored directly in table index
        uint32_t & index_value = *reinterpret_cast<uint32_t*>(
            _region_ptr + index_region_offset() + \
                loc.index_location * sizeof(uint32_t));
        index_value = new_offset;
    }
}

void hash_ring::drop_from_hash_chain(const locator & loc)
{
	SAMOA_ASSERT(loc.element_head);

    if(loc.previous_chained_head)
    {
    	uint32_t meta_cs = loc.previous_chained_head->compute_meta_checksum();
        // update previous element in the hash chain
        loc.previous_chained_head->set_hash_chain_next(
            loc.element_head->hash_chain_next());
        loc.previous_chained_head->update_meta_of_combined_checksum(meta_cs);
    }
    else
    {
    	// offset is stored directly in table index
        uint32_t & index_value = *reinterpret_cast<uint32_t*>(
            _region_ptr + index_region_offset() + \
                loc.index_location * sizeof(uint32_t));
        index_value = loc.element_head->hash_chain_next();
    }
}

packet * hash_ring::head() const
{
    if(!_tbl.is_wrapped && _tbl.begin == _tbl.end)
        return nullptr;

    return reinterpret_cast<packet*>(_region_ptr + _tbl.begin);
}

packet * hash_ring::next_packet(const packet * pkt) const
{
    uint32_t offset = packet_offset(pkt) + pkt->packet_length();

    if(_tbl.is_wrapped && offset == _region_size)
    {
        offset = ring_region_offset();
    }
    SAMOA_ASSERT(offset < _region_size);

    if(offset == _tbl.end)
        return nullptr;

    return reinterpret_cast<packet*>(_region_ptr + offset);
}

uint32_t hash_ring::packet_offset(const packet * pkt) const
{
    return std::distance<const uint8_t*>(_region_ptr,
        reinterpret_cast<const uint8_t*>(pkt));
}

}
}
}

