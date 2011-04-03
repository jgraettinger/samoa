#ifndef SAMOA_PERSISTENCE_HEAP_ROLLING_HASH_HPP
#define SAMOA_PERSISTENCE_HEAP_ROLLING_HASH_HPP

#include "samoa/persistence/rolling_hash.hpp"

namespace samoa {
namespace persistence {

class heap_rolling_hash : public rolling_hash
{
public:

    heap_rolling_hash(size_t region_size, size_t table_size)
     : rolling_hash::rolling_hash(
        new char[region_size], region_size, table_size)
    { }

    virtual ~heap_rolling_hash()
    { delete [] _region_ptr; }
};

}
}

#endif

