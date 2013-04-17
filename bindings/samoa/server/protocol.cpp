#include "pysamoa/boost_python.hpp"
#include "samoa/server/protocol.hpp"
#include "samoa/server/command_handler.hpp"

namespace samoa {
namespace server {

using namespace boost::python;

void make_protocol_bindings()
{
    class_<protocol, protocol::ptr_t, boost::noncopyable>(
        "Protocol", init<>())
        .def("set_command_handler", &protocol::set_command_handler)
        .def("get_command_handler", &protocol::get_command_handler,
            return_value_policy<copy_const_reference>());
}

}
}

