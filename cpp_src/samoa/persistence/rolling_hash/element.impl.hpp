
#include "samoa/persistence/rolling_hash/error.hpp"
#include "samoa/persistence/rolling_hash/hash_ring.hpp"
#include "samoa/persistence/rolling_hash/value_zco_adapter.hpp"

namespace samoa {
namespace persistence {
namespace rolling_hash {

template<typename KeyIterator, typename ValueIterator>
element::element(
    const hash_ring * ring, packet * pkt,
    uint32_t key_length, KeyIterator key_it,
    uint32_t value_length, ValueIterator value_it,
    uint32_t hash_chain_next)
 :  _ring(ring),
    _head(pkt),
    _last(nullptr)
{
    RING_INTEGRITY_CHECK(!pkt->continues_sequence());

    _head->set_hash_chain_next(hash_chain_next);

    while(key_length)
    {
        uint32_t cur_length = std::min(key_length,
            pkt->available_capacity());

        char * cur_it = pkt->set_key(cur_length);
        char * end_it = cur_it + cur_length;

        while(cur_it != end_it)
        {
            *(cur_it++) = *(key_it++);
        }
        key_length -= cur_length;

        if(key_length)
        {
            RING_INTEGRITY_CHECK(!pkt->completes_sequence());

            // we fully wrote key content into this packet;
            //  update it's checksum and skip to next packet
            pkt->set_crc_32(pkt->compute_crc_32());

            pkt = _ring->next_packet(pkt);
            RING_INTEGRITY_CHECK(pkt->continues_sequence());
        }
    }

    while(value_length)
    {
        uint32_t cur_length = std::min(value_length,
            pkt->available_capacity());

        char * cur_it = pkt->set_value(cur_length);
        char * end_it = cur_it + cur_length;

        while(cur_it != end_it)
        {
            *(cur_it++) = *(value_it++);
        }
        value_length -= cur_length;

        if(value_length)
        {
            RING_INTEGRITY_CHECK(!pkt->completes_sequence());

            // we fully wrote key content into this packet;
            //  update it's checksum and skip to next packet
            pkt->set_crc_32(pkt->compute_crc_32());

            pkt = _ring->next_packet(pkt);
            RING_INTEGRITY_CHECK(pkt->continues_sequence());
        }
    }

    do
    {
        pkt->set_crc_32(pkt->compute_crc_32());
        pkt = ring->next_packet(pkt);
    } while(pkt && pkt->continues_sequence());

    _last = pkt;
}

template<typename KeyIterator>
element::element(
    const hash_ring * ring, packet * pkt,
    uint32_t key_length, KeyIterator key_it,
    value_zco_adapter & value_output_adapater,
    uint32_t hash_chain_next)
 :  _ring(ring),
    _head(pkt),
    _last(nullptr)
{
    RING_INTEGRITY_CHECK(!pkt->continues_sequence());

    _head->set_hash_chain_next(hash_chain_next);

    while(key_length)
    {
        uint32_t cur_length = std::min(key_length,
            pkt->available_capacity());

        char * cur_it = pkt->set_key(cur_length);
        char * end_it = cur_it + cur_length;

        while(cur_it != end_it)
        {
            *(cur_it++) = *(key_it++);
        }
        key_length -= cur_length;

        if(key_length)
        {
            RING_INTEGRITY_CHECK(!pkt->completes_sequence());

            // value_zco_adapter will update packet checksums,
            //   so don't bother here

            pkt = _ring->next_packet(pkt);
            RING_INTEGRITY_CHECK(pkt->continues_sequence());
        }
    }

    value_output_adapater = value_zco_adapter(*this);
}

template<typename ValueIterator>
void element::set_value(
    uint32_t value_length, ValueIterator value_it)
{
    packet * pkt = _head;

    while(value_length)
    {
        uint32_t cur_length = std::min(value_length,
            pkt->available_capacity() + pkt->value_length());

        char * cur_it = pkt->set_value(cur_length);
        char * end_it = cur_it + cur_length;

        while(cur_it != end_it)
        {
            *(cur_it++) = *(value_it++);
        }
        value_length -= cur_length;

        pkt->set_crc_32(pkt->compute_crc_32());
        pkt = step(pkt);

        SAMOA_ASSERT(value_length == 0 || pkt);
    }

    while(pkt)
    {
        pkt->set_value(0);
        pkt->set_crc_32(pkt->compute_crc_32());
        pkt = step(pkt);
    }
}

}
}
}

