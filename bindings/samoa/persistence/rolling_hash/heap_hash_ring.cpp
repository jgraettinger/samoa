#include "pysamoa/boost_python.hpp"
#include "samoa/persistence/rolling_hash/heap_hash_ring.hpp"
#include "samoa/persistence/rolling_hash/hash_ring.hpp"

namespace samoa {
namespace persistence {
namespace rolling_hash {

namespace bpl = boost::python;

heap_hash_ring * py_open(size_t region_size, size_t index_size)
{
    return heap_hash_ring::open(region_size, index_size).release();
}

void make_heap_hash_ring_bindings()
{
    bpl::class_<heap_hash_ring, bpl::bases<hash_ring>,
        std::auto_ptr<heap_hash_ring>, boost::noncopyable>(
            "HeapHashRing", bpl::no_init)
        .def("open", &py_open,
            bpl::return_value_policy<bpl::manage_new_object>())
        .staticmethod("open")
        ;
}

}
}
}

