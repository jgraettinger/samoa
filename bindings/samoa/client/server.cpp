
#include <boost/python.hpp>
#include "samoa/client/server.hpp"
#include "samoa/core/stream_protocol.hpp"
#include "pysamoa/future.hpp"
#include "pysamoa/scoped_python.hpp"
#include <boost/bind.hpp>

namespace samoa {
namespace client {

namespace bpl = boost::python;
using namespace pysamoa;

///////////// connect_to support

void py_on_connect_to(
    const future::ptr_t & future,
    const boost::system::error_code & ec,
    const samoa::client::server_ptr_t & server)
{
    python_scoped_lock block;

    if(ec)
    {
        future->on_error(ec);
        return;
    }

    future->on_result(bpl::object(server));
}

future::ptr_t py_connect_to(const std::string & host, unsigned short port)
{
    future::ptr_t f(boost::make_shared<future>());
    server::connect_to(boost::bind(py_on_connect_to, f, _1, _2), host, port);
    return f;
}

//////////// schedule_request support

void py_on_schedule_request(
    const future::ptr_t & future,
    const boost::system::error_code & ec,
    const samoa::client::server_request_interface & iface)
{
    python_scoped_lock block;

    if(ec)
    {
        future->on_error(ec);
        return;
    }

    future->on_result(bpl::object(iface));
}

future::ptr_t py_schedule_request(server & s)
{
    future::ptr_t f(boost::make_shared<future>());
    s.schedule_request(boost::bind(py_on_schedule_request, f, _1, _2));
    return f;
}

//////////// finish_request support

void py_on_server_response(
    const future::ptr_t & future,
    const boost::system::error_code & ec,
    const samoa::client::server_response_interface & iface)
{
    python_scoped_lock block;

    if(ec)
    {
        future->on_error(ec);
        return;
    }

    future->on_result(bpl::object(iface));
}

future::ptr_t py_finish_request(server::request_interface & s)
{
    future::ptr_t f(boost::make_shared<future>());
    s.finish_request(boost::bind(py_on_server_response, f, _1, _2));
    return f;
}

//////////// get_response_data_blocks support

bpl::list py_get_response_data_blocks(const server::response_interface & s)
{
    bpl::list out;

    for(auto it = s.get_response_data_blocks().begin();
        it != s.get_response_data_blocks().end(); ++it)
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

////////////

void make_server_bindings()
{
    bpl::class_<server::request_interface>(
        "_Server_RequestInterface", bpl::no_init)
        .def("get_message", &server::request_interface::get_message,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("start_request", &server::request_interface::start_request)
        .def("write_interface", &server::request_interface::write_interface,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("finish_request", &py_finish_request);

    bpl::class_<server::response_interface>(
        "_Server_ResponseInterface", bpl::no_init)
        .def("get_message", &server::response_interface::get_message,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("read_interface", &server::response_interface::read_interface,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("get_error_code", &server::response_interface::get_error_code)
        .def("get_response_data_blocks", &py_get_response_data_blocks)
        .def("finish_response", &server::response_interface::finish_response);

    bpl::class_<server, server::ptr_t, boost::noncopyable,
            bpl::bases<core::stream_protocol> >("Server", bpl::no_init)
        .def("connect_to", &py_connect_to)
        .staticmethod("connect_to")
        .def("schedule_request", &py_schedule_request)
        .def("get_timeout_ms", &server::get_timeout_ms)
        .def("set_timeout_ms", &server::set_timeout_ms)
        .def("get_queue_size", &server::get_queue_size)
        .def("get_latency_ms", &server::get_latency_ms)
        .def("close", &server::close);
}

}
}
