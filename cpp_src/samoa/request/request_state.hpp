#ifndef SAMOA_REQUEST_STATE_HPP
#define SAMOA_REQUEST_STATE_HPP

#include "samoa/request/fwd.hpp"
#include "samoa/request/io_service_state.hpp"
#include "samoa/request/context_state.hpp"
#include "samoa/request/client_state.hpp"
#include "samoa/request/table_state.hpp"
#include "samoa/request/route_state.hpp"
#include "samoa/request/record_state.hpp"
#include "samoa/request/replication_state.hpp"
#include <boost/smart_ptr/enable_shared_from_this.hpp>
#include <boost/shared_ptr.hpp>

namespace samoa {
namespace request {

class state :
    private io_service_state,
    private context_state,
    private client_state,
    private table_state,
    private route_state,
    private record_state,
    private replication_state,
    public boost::enable_shared_from_this<state>
{
public:

    typedef boost::shared_ptr<state> ptr_t;

    state();
    virtual ~state();

    using io_service_state::get_io_service;

    using context_state::get_context;
    using context_state::get_cluster_state;
    using context_state::get_peer_set;
    using context_state::get_table_set;

    using client_state::get_client;
    using client_state::get_samoa_request;
    using client_state::get_request_data_blocks;
    using client_state::get_samoa_response;
    using client_state::add_response_data_block;
    void flush_response();
    void send_error(unsigned err_code, const std::string & err_msg);
    void send_error(unsigned err_code,
        const boost::system::error_code & err_msg);

    void load_table_state();

    using table_state::get_table_uuid;
    using table_state::set_table_uuid;
    using table_state::get_table_name;
    using table_state::get_table;

    void load_route_state();

    using route_state::get_key;
    using route_state::set_key;
    using route_state::get_ring_position;
    using route_state::has_primary_partition_uuid;
    using route_state::get_primary_partition_uuid;
    using route_state::set_primary_partition_uuid;
    using route_state::get_primary_partition;
    using route_state::has_peer_partition_uuids;
    using route_state::get_peer_partition_uuids;
    using route_state::get_peer_partitions;

    using record_state::get_local_record;
    using record_state::get_remote_record;

    void load_replication_state();

    using replication_state::get_quorum_count;
    using replication_state::set_quorum_count;
    using replication_state::get_peer_success_count;
    using replication_state::get_peer_failure_count;
    using replication_state::is_replication_finished;
    using replication_state::peer_replication_success;
    using replication_state::peer_replication_failure;

    /*!
     * Loads io_service_state, context_state, and client_state.
     *
     * Returns a reference to the (private) client_state,
     *  for use by samoa::server::client in populating the request
     */
    client_state & initialize_from_client(const server::client_ptr_t &);

    /*!
     * Validates and parses the protobuf SamoaRequest of the request::state
     *
     * Details of the request are loaded into the appropriate sub-states
     */
    void parse_samoa_request();    

    void reset_state();
};

}
}

#endif

