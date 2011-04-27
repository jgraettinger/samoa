
#include <boost/python.hpp>
#include "samoa/persistence/mapped_rolling_hash.hpp"

namespace samoa {
namespace persistence {

namespace bpl = boost::python;

mapped_rolling_hash * py_open(const std::string & file,
    size_t region_size, size_t table_size)
{
    std::unique_ptr<mapped_rolling_hash> p = std::move(
        mapped_rolling_hash::open(file, region_size, table_size));

    // unwrap unique_ptr: python will manage lifetime
    return p.release();
}

void make_mapped_rolling_hash_bindings()
{
    bpl::class_<mapped_rolling_hash, bpl::bases<rolling_hash>,
        std::auto_ptr<mapped_rolling_hash>, boost::noncopyable>(
            "MappedRollingHash", bpl::no_init)
        .def("open", &py_open,
            bpl::return_value_policy<bpl::manage_new_object>())
        .staticmethod("open");
}

}
}

