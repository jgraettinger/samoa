
#include <boost/python.hpp>
#include "samoa/server/listener.hpp"
#include "samoa/server/context.hpp"
#include "samoa/server/protocol.hpp"
#include <string>

namespace samoa {
namespace server {

namespace bpl = boost::python;
using namespace std;

void make_listener_bindings()
{
    bpl::class_<listener, listener::ptr_t, boost::noncopyable>("Listener",
            bpl::init<const context::ptr_t &, const protocol::ptr_t &>(
                bpl::args("context", "protocol")))
        .def("cancel", &listener::cancel)
        .def("get_address", &listener::get_address)
        .def("get_port", &listener::get_port)
        .def("get_context", &listener::get_context,
            bpl::return_value_policy<bpl::copy_const_reference>())
        .def("get_protocol", &listener::get_protocol,
            bpl::return_value_policy<bpl::copy_const_reference>())
        ;
}

}
}

