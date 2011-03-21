
#include "samoa/client/server_pool.hpp"
#include "pysamoa/future.hpp"
#include <boost/smart_ptr/make_shared.hpp>
#include <boost/python.hpp>

namespace samoa {
namespace client {

namespace bpl = boost::python;
using namespace pysamoa;
using namespace std;

future::ptr_t py_schedule_request(server_pool & s, const core::uuid & uuid)
{
    future::ptr_t f(boost::make_shared<future>());

    s.schedule_request(uuid, boost::bind(
        &future::on_server_request, f, _1, _2));
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
