
#include "samoa/core/fwd.hpp"
#include "samoa/spinlock.hpp"
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/random_device.hpp>

namespace samoa {
namespace core {
namespace random {

uint64_t generate_uint64()
{
    static boost::random::random_device random_device;
    static boost::mt19937_64 generator(random_device());
    static spinlock lock;

    spinlock::guard guard(lock);
    return generator();
}

}
}
}

