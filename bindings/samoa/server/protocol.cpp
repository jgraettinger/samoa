
#include "samoa/server/protocol.hpp"
#include "samoa/server/command_handler.hpp"
#include <boost/python.hpp>

namespace samoa {
namespace server {

using namespace boost::python;

void make_protocol_bindings()
{
    command_handler::ptr_t (protocol::*get_cmd_handler_ptr)(
        const std::string &) const = &protocol::get_command_handler;

    class_<protocol, protocol::ptr_t, boost::noncopyable>(
        "Protocol", no_init)
        .def("add_command_handler", &protocol::add_command_handler)
        .def("get_command_handler", get_cmd_handler_ptr);
}

}
}

