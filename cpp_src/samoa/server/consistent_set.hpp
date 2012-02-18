#ifndef SAMOA_SERVER_CONSISTENT_SET_HPP
#define SAMOA_SERVER_CONSISTENT_SET_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/core/fwd.hpp"

namespace samoa {
namespace server {

class consistent_set
{
public:

    typedef consistent_set_ptr_t ptr_t;

    void add(const core::murmur_checksum_t &);

    bool test(const core::murmur_checksum_t &);
};

}
}

#endif
