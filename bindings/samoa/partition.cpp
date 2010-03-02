
#include "samoa/partition.hpp"
#include "common/reactor.hpp"
#include <boost/python.hpp>
#include <string>

using namespace samoa;
using namespace boost::python;

void make_partition_bindings()
{
    class_<partition, partition::ptr_t, boost::noncopyable>("partition", init<
        const std::string &, const std::string &, size_t, size_t>())
    .add_property("uuid", make_function(&partition::get_uuid,
        return_value_policy<copy_const_reference>()))
    .def("handle_request", &partition::handle_request);
}

