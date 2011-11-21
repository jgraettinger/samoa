
#include "samoa/persistence/rolling_hash/hash_ring.hpp"
#include "samoa/persistence/rolling_hash/key_gather_iterator.hpp"
#include "samoa/persistence/rolling_hash/value_gather_iterator.hpp"
#include "samoa/core/buffer_region.hpp"
#include "samoa/error.hpp"
#include "samoa/log.hpp"
#include <boost/functional/hash.hpp>
#include <ext/algorithm>
#include <algorithm>
#include <vector>

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

    SAMOA_ASSERT(region_size > (index_size * sizeof(uint32_t) + sizeof(table_header)));

    if(_tbl.persistence_state == NEW)
    {
        _tbl.begin = ring_region_offset();
        _tbl.end = ring_region_offset();
        _tbl.is_wrapped = false;
    }
    else
    {
        SAMOA_ASSERT(_tbl.persistence_state == FROZEN);
    }
    _tbl.persistence_state = ACTIVE;
}

hash_ring::~hash_ring()
{ }

template<typename KeyIterator>
hash_ring::locator hash_ring::locate_key(
    const KeyIterator & key_begin, const KeyIterator & key_end) const
{
    locator loc = {0, nullptr, nullptr};

    size_t hash_value = boost::hash_range(key_begin, key_end);
    loc.index_location = hash_value % _index_size;

    // dereference index location to head packet offset
    uint32_t next_offset = *reinterpret_cast<uint32_t*>(
        _region_ptr + loc.index_location * sizeof(uint32_t));

    while(next_offset != 0)
    {
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

        LOG_INFO("creating packet length " << packet_length << " capacity " << capacity);

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

void hash_ring::reclaim_head()
{
    packet * pkt = head();
    SAMOA_ASSERT(pkt && pkt->is_dead());

    bool first_in_sequence = true;

    while(true)
    {
        RING_INTEGRITY_CHECK(pkt && pkt->is_dead());

        if(first_in_sequence)
        {
            RING_INTEGRITY_CHECK(!pkt->continues_sequence());
            first_in_sequence = false;
        }
        else
            RING_INTEGRITY_CHECK(pkt->continues_sequence());

        _tbl.begin += pkt->packet_length();

        if(pkt->completes_sequence())
            break;

        pkt = next_packet(pkt);
    }
}

void hash_ring::rotate_head()
{
    element old_element(this, head());

    locator loc = locate_key(
        key_gather_iterator(old_element), key_gather_iterator());
    RING_INTEGRITY_CHECK(loc.element_head);

    uint32_t key_length = old_element.key_length();
    uint32_t value_length = old_element.value_length();

    // attempt to allocate new packets for a direct copy
    packet * new_head = allocate_packets(key_length + value_length);

    if(new_head)
    {
        // construct element to assign key & value 
        element new_element(this, new_head,
            key_length, key_gather_iterator(old_element),
            value_length, value_gather_iterator(old_element));

        old_element.set_dead();
        reclaim_head();
    }
    else
    {
        // full condition: allocation failed

        // copy element key / value content to temporary buffers, mark and
        //  reclaim the element, re-allocate packets, and write from temp buffers

        std::vector<uint8_t> key_buffer;
        std::vector<uint8_t> value_buffer;

        // TODO(johng): revisit this, would be nice to use a core::buffer_ring
        key_buffer.reserve(key_length);
        value_buffer.reserve(value_length);

        std::copy(key_gather_iterator(old_element), key_gather_iterator(),
            std::back_inserter(key_buffer));
        std::copy(value_gather_iterator(old_element), value_gather_iterator(),
            std::back_inserter(value_buffer));

        old_element.set_dead();
        reclaim_head();

        new_head = allocate_packets(key_length + value_length);
        SAMOA_ASSERT(new_head);

        element new_element(this, new_head,
            key_length, key_buffer.begin(),
            value_length, value_buffer.begin());
    }

    update_hash_chain(loc, packet_offset(new_head));
}

void hash_ring::update_hash_chain(const locator & loc,
    uint32_t new_offset)
{
    if(loc.previous_chained_head)
    {
        loc.previous_chained_head->set_hash_chain_next(new_offset);
        loc.previous_chained_head->set_crc_32(
            loc.previous_chained_head->compute_crc_32());
    }
    else
    {
        // no hash chain; update index
        uint32_t & index_value = *reinterpret_cast<uint32_t*>(
            _region_ptr + loc.index_location * sizeof(uint32_t));
        index_value = new_offset;
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
    uint32_t offset = packet_offset(pkt);

    offset += pkt->packet_length();

    if(_tbl.is_wrapped)
    {
        if(offset == _region_size)
            offset = ring_region_offset();

        if(offset == _tbl.begin)
            return nullptr;
    }
    else if(offset == _tbl.end)
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

