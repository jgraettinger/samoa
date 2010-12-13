
#include "samoa/mapped_rolling_hash.hpp"
#include <boost/python.hpp>

namespace samoa {
namespace bpl = boost::python;

void make_mapped_rolling_hash_bindings()
{
    bpl::class_<mapped_rolling_hash, std::auto_ptr<mapped_rolling_hash>,
        bpl::bases<rolling_hash>, boost::noncopyable>(
            "MappedRollingHash", bpl::no_init)
        .def("open", &mapped_rolling_hash::open)
        .staticmethod("open");
}

};
