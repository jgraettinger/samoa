
#include "samoa/client/server.hpp"
#include "samoa/core/stream_protocol.hpp"
#include "pysamoa/future.hpp"
#include <boost/smart_ptr/make_shared.hpp>
#include <boost/python.hpp>

namespace samoa {
namespace client {

namespace bpl = boost::python;
using namespace pysamoa;
using namespace std;

future::ptr_t py_connect_to(
    const core::proactor::ptr_t & proactor,
    const string & host, const string & port)
{
    future::ptr_t f(boost::make_shared<future>());

    server::connect_to(proactor, host, port,
        boost::bind(&future::on_server_connect, f, _1, _2));
    return f;
}

future::ptr_t py_schedule_request(server & s)
{
    future::ptr_t f(boost::make_shared<future>());

    s.schedule_request(boost::bind(
        &future::on_server_request, f, _1, _2));
    return f;
}

future::ptr_t py_finish_request(server::request_interface & s)
{
    future::ptr_t f(boost::make_shared<future>());

    s.finish_request(boost::bind(
        &future::on_server_response, f, _1, _2));
    return f;
}

void make_server_bindings()
{
    bpl::class_<server::request_interface>(
        "_Server_RequestInterface", bpl::no_init)
        .def("get_request", &server::request_interface::get_request,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("start_request", &server::request_interface::start_request)
        .def("write_interface", &server::request_interface::write_interface,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("finish_request", &py_finish_request);

    bpl::class_<server::response_interface>(
        "_Server_ResponseInterface", bpl::no_init)
        .def("get_response", &server::response_interface::get_response,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("read_interface", &server::response_interface::read_interface,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("finish_response", &server::response_interface::finish_response);

    bpl::class_<server, server::ptr_t, boost::noncopyable,
            bpl::bases<core::stream_protocol> >("Server", bpl::no_init)
        .def("connect_to", &py_connect_to)
        .staticmethod("connect_to")
        .def("schedule_request", &py_schedule_request)
        .def("get_timeout_ms", &server::get_timeout_ms)
        .def("set_timeout_ms", &server::set_timeout_ms)
        .def("close", &server::close);
}

}
}
