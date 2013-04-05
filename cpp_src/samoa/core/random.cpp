
#include "samoa/core/fwd.hpp"
#include "samoa/spinlock.hpp"
#include <random>

namespace samoa {
namespace core {
namespace random {

uint64_t generate_uint64()
{
    static std::random_device random_device;
    static std::mt19937_64 generator(random_device());
    static spinlock lock;

    spinlock::guard guard(lock);
    return generator();
}

}
}
}

