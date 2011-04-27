
#include <boost/python.hpp>
#include "samoa/client/server_pool.hpp"
#include "pysamoa/future.hpp"
#include "pysamoa/scoped_python.hpp"
#include <boost/smart_ptr/make_shared.hpp>

namespace samoa {
namespace client {

namespace bpl = boost::python;
using namespace pysamoa;

void py_on_schedule_request(
    const future::ptr_t & future,
    const boost::system::error_code & ec,
    const samoa::client::server_request_interface & iface);

future::ptr_t py_schedule_request(server_pool & s, const core::uuid & uuid)
{
    future::ptr_t f(boost::make_shared<future>());
    s.schedule_request(boost::bind(py_on_schedule_request, f, _1, _2), uuid);
    return f;
}

void make_server_pool_bindings()
{
    bpl::class_<server_pool, server_pool::ptr_t, boost::noncopyable>(
        "ServerPool", bpl::init<core::proactor::ptr_t>(bpl::args("proactor")))
        .def("set_server_address", &server_pool::set_server_address)
        .def("set_connected_server", &server_pool::set_connected_server)
        .def("schedule_request", &py_schedule_request)
        .def("get_server", &server_pool::get_server)
        .def("close", &server_pool::close);
}

}
}
