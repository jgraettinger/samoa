
#include <boost/python.hpp>
#include "samoa/server/client.hpp"
#include "samoa/server/protocol.hpp"
#include "samoa/server/context.hpp"
#include "samoa/core/stream_protocol.hpp"
#include "pysamoa/future.hpp"
#include "pysamoa/scoped_python.hpp"
#include <boost/bind.hpp>

namespace samoa {
namespace server {

namespace bpl = boost::python;
using namespace pysamoa;

//////////// schedule_response support

void py_on_schedule_response(
    const future::ptr_t & future,
    const samoa::server::client::response_interface & iface)
{
    python_scoped_lock block;

    future->on_result(bpl::object(iface));
}

future::ptr_t py_schedule_response(client & c)
{
    future::ptr_t f(boost::make_shared<future>());
    c.schedule_response(boost::bind(py_on_schedule_response, f, _1));
    return f;
}

////////////

std::string py_repr(const client & c)
{
    return "client@<" + \
        c.get_remote_address() + ":" + \
        boost::lexical_cast<std::string>(c.get_remote_port()) + \
        ">";
}

void make_client_bindings()
{
    bpl::class_<client::response_interface>(
        "_Client_ResponseInterface", bpl::no_init)
        .def("write_interface", &client::response_interface::write_interface,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("finish_response", &client::response_interface::finish_response);

    bpl::class_<client, client::ptr_t, boost::noncopyable,
            bpl::bases<core::stream_protocol> >("Client", bpl::no_init)
        .def("get_context", &client::get_context,
            bpl::return_value_policy<bpl::copy_const_reference>())
        .def("get_protocol", &client::get_protocol,
            bpl::return_value_policy<bpl::copy_const_reference>())
        .def_readonly("max_request_concurrency",
            &client::max_request_concurrency)
        .def("schedule_response", &py_schedule_response)
        .def("__repr__", &py_repr);
}

}
}
