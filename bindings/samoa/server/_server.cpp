
#include <boost/python.hpp>

namespace samoa {
namespace server {
    void make_client_bindings();
    void make_context_bindings();
    void make_listener_bindings();
    void make_protocol_bindings();
    void make_simple_protocol_bindings();
    void make_command_handler_bindings();
}
}

BOOST_PYTHON_MODULE(_server)
{
    samoa::server::make_client_bindings();
    samoa::server::make_context_bindings();
    samoa::server::make_listener_bindings();
    samoa::server::make_protocol_bindings();
    samoa::server::make_simple_protocol_bindings();
    samoa::server::make_command_handler_bindings();
}

