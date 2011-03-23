
#include "samoa/server/client.hpp"
#include "samoa/server/protocol.hpp"
#include "samoa/server/context.hpp"
#include "samoa/core/stream_protocol.hpp"
#include <boost/python.hpp>

namespace samoa {
namespace server {

namespace bpl = boost::python;

std::string py_repr(const client & c)
{
    return "client@<" + \
        c.get_remote_address() + ":" + \
        boost::lexical_cast<std::string>(c.get_remote_port()) + \
        ">";
}

void make_client_bindings()
{
    bpl::class_<client, client::ptr_t, boost::noncopyable,
            bpl::bases<core::stream_protocol> >("Client", bpl::no_init)
        .def("get_context", &client::get_context,
            bpl::return_value_policy<bpl::copy_const_reference>())
        .def("get_protocol", &client::get_protocol,
            bpl::return_value_policy<bpl::copy_const_reference>())
        .def("get_request", &client::get_request,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("read_interface", &client::read_interface,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("get_response", &client::get_response,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("set_error", &client::set_error)
        .def("start_response", &client::start_response)
        .def("write_interface", &client::write_interface,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("finish_response", &client::finish_response)
        .def("__repr__", &py_repr);
}

}
}
