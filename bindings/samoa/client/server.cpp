
#include <boost/python.hpp>
#include "samoa/client/server.hpp"
#include "pysamoa/future.hpp"
#include "pysamoa/scoped_python.hpp"
#include <functional>

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
    server::connect_to(std::bind(py_on_connect_to, f,
        std::placeholders::_1, std::placeholders::_2), host, port);
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
    s.schedule_request(std::bind(py_on_schedule_request, f,
        std::placeholders::_1, std::placeholders::_2));
    return f;
}

//////////// add_data_block support

void py_add_data_block(server::request_interface & iface, const bpl::str & py_str)
{
    char * cbuf;
    Py_ssize_t length;

    if(PyString_AsStringAndSize(py_str.ptr(), &cbuf, &length) == -1)
        bpl::throw_error_already_set();

    iface.add_data_block(cbuf, cbuf + length);
}

//////////// flush_request support

void py_on_server_response(
    const future::ptr_t & future,
    const boost::system::error_code & ec,
    const server_response_interface & iface)
{
    python_scoped_lock block;

    if(ec)
    {
        future->on_error(ec);
        return;
    }

    future->on_result(bpl::object(iface));
}

future::ptr_t py_flush_request(server::request_interface & iface)
{
    future::ptr_t f(boost::make_shared<future>());
    iface.flush_request(std::bind(py_on_server_response, f,
        std::placeholders::_1, std::placeholders::_2));
    return f;
}

//////////// get_response_data_blocks support

bpl::list py_get_response_data_blocks(const server::response_interface & iface)
{
    bpl::list out;

    for(auto it = iface.get_response_data_blocks().begin();
        it != iface.get_response_data_blocks().end(); ++it)
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
        .def("add_data_block", &py_add_data_block)
        .def("flush_request", &py_flush_request);

    bpl::class_<server::response_interface>(
        "_Server_ResponseInterface", bpl::no_init)
        .def("get_message", &server::response_interface::get_message,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("get_error_code", &server::response_interface::get_error_code)
        .def("get_response_data_blocks", &py_get_response_data_blocks)
        .def("finish_response", &server::response_interface::finish_response);

    bpl::class_<server, server::ptr_t, boost::noncopyable>(
            "Server", bpl::no_init)
        .def("connect_to", &py_connect_to)
        .staticmethod("connect_to")
        .def("schedule_request", &py_schedule_request)
        ;
}

}
}
