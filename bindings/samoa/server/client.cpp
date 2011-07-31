
#include <boost/python.hpp>
#include "samoa/server/client.hpp"
#include "samoa/server/protocol.hpp"
#include "samoa/server/context.hpp"
#include "samoa/core/stream_protocol.hpp"

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

bpl::list py_get_request_data_blocks(const client & c)
{
    bpl::list out;

    for(auto it = c.get_request_data_blocks().begin();
        it != c.get_request_data_blocks().end(); ++it)
    {
        unsigned len = 0;
        for(auto it2 = it->begin(); it2 != it->end(); ++it2)
        {
            len += it2->size();
        }

        bpl::str buffer(bpl::handle<>(
            PyString_FromStringAndSize(0, len)));

        char * out_it = PyString_AS_STRING(buffer.ptr());
        for(auto it2 = it->begin(); it2 != it->end(); ++it2)
        {
            out_it = std::copy(it2->begin(), it2->end(), out_it);
        }
        out.append(buffer);
    }

    return out;
}

void make_client_bindings()
{
    void (client::*p_send_error)(unsigned, const std::string &, bool) = &client::send_error;

    bpl::class_<client, client::ptr_t, boost::noncopyable,
            bpl::bases<core::stream_protocol> >("Client", bpl::no_init)
        .def("get_context", &client::get_context,
            bpl::return_value_policy<bpl::copy_const_reference>())
        .def("get_protocol", &client::get_protocol,
            bpl::return_value_policy<bpl::copy_const_reference>())
        .def("get_request", &client::get_request,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("get_request_data_blocks", &py_get_request_data_blocks)
        .def("read_interface", &client::read_interface,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("get_response", &client::get_response,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("send_error", p_send_error,
            (bpl::arg("error_code"),
             bpl::arg("error_message"),
             bpl::arg("closing") = false))
        .def("start_response", &client::start_response)
        .def("write_interface", &client::write_interface,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("finish_response", &client::finish_response)
        .def("__repr__", &py_repr);
}

}
}
