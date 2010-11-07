
#include "samoa/partition_router.hpp"
#include <boost/python.hpp>
#include <string>

using namespace samoa;
using namespace boost::python;

void make_partition_router_bindings()
{
    class_<partition_router, partition_router::ptr_t, boost::noncopyable>(
        "partition_router", no_init)
    .def("route_request", &partition_router::route_request);
}

