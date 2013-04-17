#include "pysamoa/boost_python.hpp"
#include "samoa/persistence/rolling_hash/mapped_hash_ring.hpp"
#include "samoa/persistence/rolling_hash/hash_ring.hpp"

namespace samoa {
namespace persistence {
namespace rolling_hash {

namespace bpl = boost::python;

mapped_hash_ring * py_open(const std::string & file,
    size_t region_size, size_t table_size)
{
    std::unique_ptr<mapped_hash_ring> p = std::move(
        mapped_hash_ring::open(file, region_size, table_size));

    // unwrap unique_ptr: python will manage lifetime
    return p.release();
}

void make_mapped_hash_ring_bindings()
{
    bpl::class_<mapped_hash_ring, bpl::bases<hash_ring>,
        std::auto_ptr<mapped_hash_ring>, boost::noncopyable>(
            "MappedHashRing", bpl::no_init)
        .def("open", &py_open,
            bpl::return_value_policy<bpl::manage_new_object>())
        .staticmethod("open")
        ;
}

}
}
}

