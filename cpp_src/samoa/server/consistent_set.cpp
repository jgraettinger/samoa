#include "samoa/server/consistent_set.hpp"
#include "samoa/log.hpp"

namespace samoa {
namespace server {

void consistent_set::add(const core::murmur_checksum_t &)
{

}

bool consistent_set::test(const core::murmur_checksum_t &)
{
    return false;    
}

}
}

