

template<typename KeyIterator>
hash_ring::locator hash_ring::locate_key(
    const KeyIterator & key_begin, const KeyIterator & key_end) const
{
    locator loc = {0, nullptr, nullptr};

    size_t hash_value = boost::hash_range(std::begin(key), std::end(key));
    loc.index_location = hash_value % _index_size;

    // dereference index location to head packet offset
    uint32_t next_offset = *reinterpret_cast<uint32_t*>(
        _region_ptr + loc.index_location * sizeof(uint32_t));

    while(next_offset != 0)
    {
        // follow chain to next packet
        loc.previous_chained_head = loc.element_head;
        loc.element_head = reinterpret_cast<packet*>(
            _region_ptr + next_offset)

        if(!std::lexicographical_compare_3way(
            std::begin(key), std::end(key),
            key_gather_iterator(loc.element_head, this),
            key_gather_iterator()))
        {
            // keys match
            return loc;
        }
        next_offset = loc.element_head->get_hash_chain_next();
    }

    // key not found in chain
    loc.previous_chained_head = loc.element_head;
    loc.element_head = nullptr;

    return loc;
}

hash_ring::locator hash_ring::allocate_packets(uint32_t capacity) const
{
    locator loc = {0, nullptr, nullptr}

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
                loc.element_head = nullptr;
                return loc;
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
        SAMOA_ASSERT(block_length >= packet::min_packet_byte_length);
        // TODO debug invariant
        SAMOA_ASSERT(!(block_length % sizeof(uint32_t)));

        uint32_t packet_length = std::min(block_length,
            packet::header_length() + std::min(capacity, packet::max_capacity));

        // round up to uint32_t-aligned length
        //   this may only happen if capacity bounds packet-length
        if(packet_length % sizeof(uint32_t))
        {
            packet_length += sizeof(uint32_t) - (packet_length % sizeof(uint32_t));
        }

        uint32_t remainder = block_length - packet_length;
        if(remainder && remainder < packet::min_packet_byte_length)
        {
            if(packet_length + remainder > packet::max_packet_byte_length)
            {
                // shorten packet, so that a minimum length packet may be written
                packet_length -= packet::min_packet_byte_length + remainder;
            }
            else
            {
                // elongate packet to consume remainder
                packet_length += remainder;
            }
        }

        // initialize packet
        packet * pkt = reinterpret_cast<packet*>(_region_ptr + cur_end);
        new (pkt) packet(packet_length - packet::header_length());

        if(first_in_sequence)
        {
            first_in_sequence = false;
            loc.element_head = pkt;
        }
        else
            pkt->set_continues_sequence();

        if(pkt->get_capacity() == capacity)
            pkt->set_completes_sequence();

        capacity -= pkt->get_capacity();

        if(cur_end + packet_length == _region_size)
        {
            // we wrapped around the ring end
            cur_is_wrapped = true;
            cur_end = ring_begin_offset();
        }
    }
    _tbl.is_wrapped = cur_is_wrapped;
    _tbl.end = cur_end;

    return loc;
}

void hash_ring::reclaim_head()
{
    packet_t * packet = head();
    SAMOA_ASSERT(packet && packet->is_dead());

    bool first_in_sequence = true;

    while(true)
    {
        RING_INTEGRITY_CHECK(packet && packet->is_dead());

        if(first_in_sequence)
        {
            RING_INTEGRITY_CHECK(!packet.continues_sequence());
            first_in_sequence = false;
        }
        else
            RING_INTEGRITY_CHECK(packet.continues_sequence());

        _tbl.begin += packet->get_packet_length();

        if(packet->completes_sequence())
            break;

        packet = next_packet(packet);
    }
}

template<typename KeyIterator>
packet_t * hash_ring::assign_key(packet_t * packet,
    uint32_t key_length, KeyIterator key_it)
{
    while(key_length)
    {
        uint32_t cur_length = std::min(key_length,
            packet->get_available_capacity());

        char * cur_it = packet->set_key(cur_length);
        char * end_it = cur_it + cur_length;

        key_length -= cur_length;

        while(cur_it != end_it)
        {
            *(cur_it++) = *(key_it++);
        }

        if(key_length)
        {
            packet = next_packet(packet);
        }
    }
    return packet;
}

template<typename ValueIterator>
packet_t * hash_ring::assign_value(packet_t * packet,
    uint32_t value_length, ValueIterator value_it)
{
    while(value_length)
    {
        uint32_t cur_length = std::min(value_length,
            packet->get_available_capacity());

        char * cur_it = packet->set_value(cur_length);
        char * end_it = cur_it + cur_length;

        value_length -= cur_length;

        while(cur_it != end_it)
        {
            *(cur_it++) = *(value_it++);
        }

        if(value_length)
        {
            packet = next_packet(packet);
        }
    }
    return packet;
}

void hash_ring::rotate_head()
{
    element_t element(this, head());
    uint32_t key_length = element.get_key_length();
    uint32_t value_length = element.get_value_length();

    // attempt to allocate new packets for a direct copy
    packet_t * packet = allocate_packets(key_length + value_length);

    if(packet)
    {
        element_t new_element(this, packet);

        new_element.assign_key(key_length, element.key_begin());
        new_element.assign_value(value_length, element.value_begin());
        new_element.compute_checksum();

        element.mark_as_dead();
        reclaim_head();
    }
    else
    {
        // full condition: allocation failed

        // copy element key / value content to temporary buffers,
        //  drop the element, and re-allocate packets on the 

        core::buffer_ring buffer_ring;
        core::buffer_regions_t key_buffers;
        core::buffer_regions_t value_buffers;

        buffer_ring.produce_range(
            element.key_begin(), element.key_end());
        buffer_ring.get_read_regions(key_buffers);
        buffer_ring.consumed(_buffer_ring.available_read());

        buffer_ring.produce_range(
            element.value_begin(), element.value_end());
        buffer_ring.get_read_regions(value_buffers);
        buffer_ring.consumed(_buffer_ring.available_read());

        element.mark_as_dead();
        reclaim_head();

        tmp = packet = allocate_packets(key_length + value_length);
        SAMOA_ASSERT(packet);

        tmp = assign_key(tmp, key_length,
            asio::buffers_iterator::begin(key_buffers));
        tmp = assign_value(tmp, value_length,
            asio::buffers_iterator::begin(value_buffers));

        SAMOA_ASSERT(tmp->completes_sequence());
    }
    


}

