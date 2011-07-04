#ifndef SAMOA_SERVER_FWD_HPP
#define SAMOA_SERVER_FWD_HPP

#include <boost/shared_ptr.hpp>
#include <boost/smart_ptr/enable_shared_from_this.hpp>
#include <boost/smart_ptr/make_shared.hpp>

namespace samoa {
namespace server {

class context;
typedef boost::shared_ptr<context> context_ptr_t;
typedef boost::weak_ptr<context> context_weak_ptr_t;

class protocol;
typedef boost::shared_ptr<protocol> protocol_ptr_t;

class listener;
typedef boost::shared_ptr<listener> listener_ptr_t;

class client;
typedef boost::shared_ptr<client> client_ptr_t;

class command_handler;
typedef boost::shared_ptr<command_handler> command_handler_ptr_t;

class cluster_state;
typedef boost::shared_ptr<cluster_state> cluster_state_ptr_t;

class table_set;
typedef boost::shared_ptr<table_set> table_set_ptr_t;

class peer_set;
typedef boost::shared_ptr<peer_set> peer_set_ptr_t;

class table;
typedef boost::shared_ptr<table> table_ptr_t;

class partition;
typedef boost::shared_ptr<partition> partition_ptr_t;

class local_partition;
typedef boost::shared_ptr<local_partition> local_partition_ptr_t;

class remote_partition;
typedef boost::shared_ptr<remote_partition> remote_partition_ptr_t;

class peer_discovery;
typedef boost::shared_ptr<peer_discovery> peer_discovery_ptr_t;

}
}

#endif

