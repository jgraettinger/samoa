#ifndef SAMOA_SERVER_FWD_HPP
#define SAMOA_SERVER_FWD_HPP

#include <memory>

namespace samoa {
namespace server {

class context;
typedef std::shared_ptr<context> context_ptr_t;
typedef std::weak_ptr<context> context_weak_ptr_t;

class protocol;
typedef std::shared_ptr<protocol> protocol_ptr_t;

class listener;
typedef std::shared_ptr<listener> listener_ptr_t;
typedef std::weak_ptr<listener> listener_weak_ptr_t;

class client;
typedef std::shared_ptr<client> client_ptr_t;
typedef std::weak_ptr<client> client_weak_ptr_t;

class client_response_interface;

class command_handler;
typedef std::shared_ptr<command_handler> command_handler_ptr_t;

class cluster_state;
typedef std::shared_ptr<cluster_state> cluster_state_ptr_t;

class table_set;
typedef std::shared_ptr<table_set> table_set_ptr_t;

class peer_set;
typedef std::shared_ptr<peer_set> peer_set_ptr_t;

class table;
typedef std::shared_ptr<table> table_ptr_t;

class partition;
typedef std::shared_ptr<partition> partition_ptr_t;

class local_partition;
typedef std::shared_ptr<local_partition> local_partition_ptr_t;

class remote_partition;
typedef std::shared_ptr<remote_partition> remote_partition_ptr_t;

class peer_discovery;
typedef std::shared_ptr<peer_discovery> peer_discovery_ptr_t;

class digest;
typedef std::shared_ptr<digest> digest_ptr_t;

class cron;
typedef std::shared_ptr<cron> cron_ptr_t;

}
}

#endif

