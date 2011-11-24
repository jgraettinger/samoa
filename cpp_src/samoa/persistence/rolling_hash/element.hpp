#ifndef SAMOA_PERSISTENCE_ROLLING_HASH_ELEMENT_HPP
#define SAMOA_PERSISTENCE_ROLLING_HASH_ELEMENT_HPP

#include "samoa/persistence/rolling_hash/fwd.hpp"
#include "samoa/persistence/rolling_hash/packet.hpp"

namespace samoa {
namespace persistence {
namespace rolling_hash {

class element
{
public:

    element(const hash_ring *, packet *);

    template<typename KeyIterator>
    element(const hash_ring *, packet *,
        uint32_t key_length, KeyIterator key_begin,
        uint32_t hash_chain_next);
    
    template<typename KeyIterator, typename ValueIterator>
    element(const hash_ring *, packet *,
        uint32_t key_length, KeyIterator key_begin,
        uint32_t value_length, ValueIterator value_begin,
        uint32_t hash_chain_next);

    uint32_t key_length() const;
    uint32_t value_length() const;
    uint32_t capacity() const;

    template<typename ValueIterator>
    void set_value(uint32_t value_length, ValueIterator value_begin);

    //value_zco_adapter build_value_zco_adapter();
    //value_zci_adapter build_value_zci_adapter();

    void set_dead();

    // access to the underlying packet sequence
    packet * head() const
    { return _head; }

    packet * step(packet *) const;

private:

    friend class key_gather_iterator;
    friend class value_gather_iterator;
    //friend class value_zco_adapter;
    //friend class value_zci_adapter;

    const hash_ring * _ring;
    packet * _head;
    mutable packet * _last;
};

}
}
}

#include "samoa/persistence/rolling_hash/element.impl.hpp"

#endif

