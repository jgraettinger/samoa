#include <boost/python.hpp>

void make_reactor_bindings();
void make_server_bindings();
void make_client_protocol_bindings();
void make_partition_bindings();
void make_partition_router_bindings();

BOOST_PYTHON_MODULE(_samoa)
{
    make_reactor_bindings();
    make_server_bindings();
    make_client_protocol_bindings();
    make_partition_bindings();
    make_partition_router_bindings();
}
