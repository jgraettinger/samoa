
#include "samoa/server/client.hpp"
#include "samoa/server/protocol.hpp"
#include "samoa/server/context.hpp"
#include "samoa/core/stream_protocol.hpp"
#include <boost/python.hpp>

namespace samoa {
namespace server {

using namespace boost::python;

void make_client_bindings()
{
    class_<client, client::ptr_t, boost::noncopyable,
            bases<core::stream_protocol> >("Client", no_init)
        .def("start_next_request", &client::start_next_request)
        .add_property("context", make_function(&client::get_context,
            return_value_policy<copy_const_reference>()))
        .add_property("protocol", make_function(&client::get_protocol,
            return_value_policy<copy_const_reference>()));
}

}
}
