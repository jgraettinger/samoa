
#include <boost/python.hpp>

namespace samoa {
namespace server {
namespace command {
    void make_cluster_state_handler_bindings();
    void make_get_blob_handler_bindings();
    void make_set_blob_handler_bindings();
    void make_update_counter_handler_bindings();
    void make_counter_value_handler_bindings();
    void make_replicate_handler_bindings();
    void make_digest_sync_handler_bindings();
}
}
}

BOOST_PYTHON_MODULE(_command)
{
    samoa::server::command::make_cluster_state_handler_bindings();
    samoa::server::command::make_get_blob_handler_bindings();
    samoa::server::command::make_set_blob_handler_bindings();
    samoa::server::command::make_update_counter_handler_bindings();
    samoa::server::command::make_counter_value_handler_bindings();
    samoa::server::command::make_replicate_handler_bindings();
    samoa::server::command::make_digest_sync_handler_bindings();
}
