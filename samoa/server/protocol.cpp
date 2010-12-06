
#include "samoa/server/protocol.hpp"
#include "samoa/server/command_handler.hpp"
#include <boost/python.hpp>

namespace server {

using namespace boost::python;

typedef boost::unordered_map<std::string,
    command_handler_ptr_t> cmd_table_t;

void protocol::add_command_handler(const std::string & cmd_name,
    const command_handler::ptr_t & cmd)
{
    if(_cmd_table.insert(std::make_pair(cmd_name, cmd)).second != true)
    {
        throw std::runtime_error(
            "command " + cmd_name + " already has a handler");
    }
}

command_handler_ptr_t protocol::get_command_handler(
    const std::string & cmd_name) const
{
    cmd_table_t::const_iterator it = _cmd_table.find(cmd_name);

    if(it != _cmd_table.end())
        return it->second;

    return command_handler_ptr_t();
}

void make_protocol_bindings()
{
    class_<protocol, protocol::ptr_t, boost::noncopyable>(
        "protocol", no_init)
        .def("add_command_handler", &protocol::add_command_handler)
        .def("get_command_handler", &protocol::get_command_handler);
}

}

