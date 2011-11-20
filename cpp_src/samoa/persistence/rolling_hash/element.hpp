#ifndef SAMOA_PERSISTENCE_ROLLING_HASH_ELEMENT_HPP
#define SAMOA_PERSISTENCE_ROLLING_HASH_ELEMENT_HPP

namespace samoa {
namespace persistence {
namespace rolling_hash {

class hash_ring_element
{
public:

    hash_ring_element(const hash_ring *, const packet *);



private:

    uint32_t _key_length;
    uint32_t _value_length;
}

}
}
}

#endif

