
#include "samoa/persistence/rolling_hash/element.hpp"
#include "samoa/persistence/rolling_hash/hash_ring.hpp"
#include "samoa/persistence/rolling_hash/error.hpp"

namespace samoa {
namespace persistence {
namespace rolling_hash {

element::element(
    const hash_ring * ring, packet * packet)
 :  _ring(ring),
    _head(packet),
    _last(packet)
{
    RING_INTEGRITY_CHECK(packet->check_integrity());
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

void element::set_dead()
{
    packet * pkt = _head;

    while(pkt)
    {
        pkt->set_dead();
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
        RING_INTEGRITY_CHECK(next->check_integrity());

        _last = next;
    }
    return next;
}

}
}
}

