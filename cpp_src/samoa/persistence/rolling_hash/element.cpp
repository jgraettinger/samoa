
#include "samoa/persistence/rolling_hash/element.hpp"
#include "samoa/persistence/rolling_hash/hash_ring.hpp"
#include "samoa/persistence/rolling_hash/error.hpp"

namespace samoa {
namespace persistence {
namespace rolling_hash {

element::element()
 : _ring(nullptr),
   _head(nullptr),
   _last(nullptr)
{ }

element::element(const hash_ring * ring, packet * head)
 :  _ring(ring),
    _head(head),
    _last(head)
{
    RING_INTEGRITY_CHECK(head->check_integrity(_content_crc));
}

element::element(
    const hash_ring * ring, packet * pkt, const std::string & key)
 :  _ring(ring),
    _head(pkt),
    _last(nullptr)
{
    RING_INTEGRITY_CHECK(!pkt->continues_sequence());

    uint32_t key_length = key.size();
    auto key_it = key.begin();

    while(key_length)
    {
        uint32_t cur_length = std::min(key_length,
            pkt->available_capacity());

        std::copy(key_it, key_it + cur_length,
            pkt->set_key(cur_length));

        key_it += cur_length;
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
    // client is responsible for computing updated checksums
    //  (likely via value_zco_adapater)
}

uint32_t element::key_length() const
{
    packet * pkt = _head;
    uint32_t length = 0;

    while(true)
    {
        length += pkt->key_length();

        if(pkt->completes_sequence() || !pkt->key_length())
            return length;

        pkt = step(pkt);
    }
}

uint32_t element::value_length() const
{
    packet * pkt = _head;
    uint32_t length = 0;

    while(true)
    {
        length += pkt->value_length();

        if(pkt->completes_sequence())
            return length;

        pkt = step(pkt);
    }
}

uint32_t element::capacity() const
{
    packet * pkt = _head;
    uint32_t length = 0;

    while(true)
    {
        length += pkt->capacity();

        if(pkt->completes_sequence())
            return length;

        pkt = step(pkt);
    } 
}

uint32_t element::content_checksum() const
{
    while(!_last->completes_sequence())
    {
        step(_last);
    }
    return _content_crc.checksum();
}

void element::set_dead()
{
    packet * pkt = _head;

    while(pkt)
    {
    	uint32_t meta_cs = pkt->compute_meta_checksum();
        pkt->set_dead();
        pkt->update_meta_of_combined_checksum(meta_cs);

        pkt = step(pkt);
    }
}

packet * element::step(packet * pkt) const
{
    if(pkt->completes_sequence())
        return nullptr;

    packet * next = _ring->next_packet(pkt);

    if(pkt == _last)
    {
        RING_INTEGRITY_CHECK(next->continues_sequence());
        RING_INTEGRITY_CHECK(next->check_integrity(_content_crc));

        _last = next;
    }
    return next;
}

}
}
}

