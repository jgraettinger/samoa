
#include "samoa/client/server.hpp"
#include "samoa/core/stream_protocol.hpp"
#include "pysamoa/future.hpp"
#include <boost/python.hpp>

namespace samoa {
namespace client {

using namespace boost::python;
using namespace pysamoa;
using namespace std;

future::ptr_t py_connect_to(
    const core::proactor::ptr_t & proactor,
    const string & host, const string & port)
{
    future::ptr_t f(new future());

    server::connect_to(proactor, host, port,
        boost::bind(&future::on_server_result, f, _1, _2));
    return f;
}

void make_server_bindings()
{
    class_<server, server::ptr_t, boost::noncopyable,
            bases<core::stream_protocol> >("Server", no_init)
        .def("connect_to", &py_connect_to)
        .staticmethod("connect_to");
}

}
}
