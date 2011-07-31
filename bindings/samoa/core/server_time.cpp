
#include <boost/python.hpp>
#include "samoa/core/server_time.hpp"

namespace samoa {
namespace core {

namespace bpl = boost::python;

void make_server_time_bindings()
{
    bpl::class_<server_time>("ServerTime", bpl::no_init)
        .def("get_time", &server_time::get_time)
        .staticmethod("get_time")
        .def("set_time", &server_time::set_time)
        .staticmethod("set_time")
        ;
}

}
}

