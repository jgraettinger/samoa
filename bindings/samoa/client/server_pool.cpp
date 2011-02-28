
#include "samoa/client/server_pool.hpp"
#include "pysamoa/future.hpp"
#include <boost/smart_ptr/make_shared.hpp>
#include <boost/python.hpp>

namespace samoa {
namespace client {

namespace bpl = boost::python;
using namespace pysamoa;
using namespace std;

future::ptr_t py_schedule_request(server_pool & s,
    const std::string & host, unsigned port)
{
    future::ptr_t f(boost::make_shared<future>());

    s.schedule_request(host, port, boost::bind(
        &future::on_server_request, f, _1, _2));
    return f;
}

void make_server_pool_bindings()
{
    bpl::class_<server_pool, server_pool::ptr_t, boost::noncopyable>(
        "ServerPool", bpl::init<core::proactor::ptr_t>())
        .def("schedule_request", &py_schedule_request);
}

}
}
