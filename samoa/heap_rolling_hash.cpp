
#include "samoa/heap_rolling_hash.hpp"
#include "samoa/rolling_hash.hpp"
#include <boost/python.hpp>

namespace samoa {
namespace bpl = boost::python;

void make_heap_rolling_hash_bindings()
{
    bpl::class_<heap_rolling_hash, bpl::bases<rolling_hash>, boost::noncopyable>(
            "HeapRollingHash", bpl::init<size_t, size_t>());
}

};
