
#include <boost/python.hpp>

namespace server {
    void make_client_bindings();
    void make_context_bindings();
    void make_listener_bindings();
    void make_protocol_bindings();
    void make_simple_protocol_bindings();
    void make_client_bindings();
};

BOOST_PYTHON_MODULE(_server)
{
    server::make_client_bindings();
    server::make_context_bindings();
    server::make_listener_bindings();
    server::make_protocol_bindings();
    server::make_simple_protocol_bindings();
    server::make_client_bindings();
}

