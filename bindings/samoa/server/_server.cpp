
#include <boost/python.hpp>

namespace samoa {
namespace server {
    void make_client_bindings();
    void make_context_bindings();
    void make_listener_bindings();
    void make_protocol_bindings();
    void make_command_handler_bindings();
    void make_cluster_state_bindings();
    void make_peer_set_bindings();
    void make_table_set_bindings();
    void make_table_bindings();
    void make_partition_bindings();
    void make_local_partition_bindings();
    void make_remote_partition_bindings();
    void make_partition_upkeep_bindings();
}
}

BOOST_PYTHON_MODULE(_server)
{
    samoa::server::make_client_bindings();
    samoa::server::make_context_bindings();
    samoa::server::make_listener_bindings();
    samoa::server::make_protocol_bindings();
    samoa::server::make_command_handler_bindings();
    samoa::server::make_cluster_state_bindings();
    samoa::server::make_peer_set_bindings();
    samoa::server::make_table_set_bindings();
    samoa::server::make_table_bindings();
    samoa::server::make_partition_bindings();
    samoa::server::make_local_partition_bindings();
    samoa::server::make_remote_partition_bindings();
    samoa::server::make_partition_upkeep_bindings();
}

