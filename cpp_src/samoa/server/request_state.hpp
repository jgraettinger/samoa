#ifndef SAMOA_SERVER_REQUEST_STATE_HPP
#define SAMOA_SERVER_REQUEST_STATE_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/server/partition_peer.hpp"
#include "samoa/client/fwd.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include "samoa/core/protobuf_helpers.hpp"
#include "samoa/core/proactor.hpp"
#include <boost/smart_ptr/enable_shared_from_this.hpp>
#include <boost/shared_ptr.hpp>

namespace samoa {
namespace server {

namespace spb = samoa::core::protobuf;

class request_state : 
    public boost::enable_shared_from_this<request_state>
{
public:

    typedef boost::shared_ptr<request_state> ptr_t;

    request_state(const server::client_ptr_t &);

    bool load_from_samoa_request(const context_ptr_t &);

    core::protobuf::SamoaRequest & get_samoa_request()
    { return _samoa_request; }

    core::protobuf::SamoaResponse & get_samoa_response()
    { return _samoa_response; }

    std::vector<core::buffer_regions_t> & get_request_data_blocks()
    { return _request_data_blocks; }

    const client_ptr_t & get_client() const
    { return _client; }

    const context_ptr_t & get_context() const
    { return _context; }

    const peer_set_ptr_t & get_peer_set() const
    { return _peer_set; }

    const table_ptr_t & get_table() const
    { return _table; }

    const std::string & get_key() const
    { return _key; }

    const local_partition_ptr_t & get_primary_partition() const
    { return _primary_partition; }

    const partition_peers_t & get_partition_peers() const
    { return _partition_peers; }

    spb::PersistedRecord & get_local_record()
    { return _local_record; }

    spb::PersistedRecord & get_remote_record()
    { return _remote_record; }

    unsigned get_client_quorum() const
    { return _client_quorum; }

    unsigned get_peer_error_count() const
    { return _error_count; }

    unsigned get_peer_success_count() const
    { return _success_count; }

    const core::io_service_ptr_t & get_io_service() const
    { return _io_srv; }

    /*!
     * \brief To be called on failed peer replication.
     *
     * Increments peer_error_count, and returns true iff the client should be
     * responded to as a result of this specific completion. Eg, because we
     * haven't yet responded, and this completion was the last remaining
     * replication.
     * 
     * Note: Only one of peer_replication_failure() or
     *  peer_replication_success() will return True for a
     *  given request_state.
     */
    bool peer_replication_failure();

    /*!
     *  \brief To be called on successful peer replication.
     *  
     * Increments peer_success, and returns true iff the client should be
     * responded to as a result of this specific completion. Eg, because
     * this completion caused us to meet the client-requested quorum, or
     * because this completion was the last remaining replication.
     *
     * Note: Only one of peer_replication_failure() or
     *  peer_replication_success() will return True for a
     *  given request_state.
     */
    bool peer_replication_success();

    /*!
     * Indicates whether one of peer_replication_failure() or
     * peer_replication_success() have returned True, and the client's
     * quorum has already been met (or failed).
     */
    bool is_client_quorum_met() const;

    /*!
     * \brief Adds the const buffer-regions as a response datablock
     *
     * SamoaResponse::data_block_length is appropriately updated.
     */
    void add_response_data_block(const core::const_buffer_regions_t &);

    /*!
     * \brief Adds the buffer-regions as a response datablock
     *
     * SamoaResponse::data_block_length is appropriately updated.
     */
    void add_response_data_block(const core::buffer_regions_t &);

    /*!
     * \brief Adds the (byte) iteration-range as a response datablock
     *
     * SamoaResponse::data_block_length is appropriately updated.
     */
    template<typename Iterator>
    void add_response_data_block(const Iterator & beg, const Iterator & end);

    /*!
     * \brief Writes the request_state's SamoaResponse and
     *  response datablocks to the client.
     *
     * Postcondition note: After invoking flush_client_response(),
     *  it is an error to call add_response_data_block(), or to
     *  mutate the SamoaResponse
     */
    void flush_client_response();

    /*!
     * \brief Helper for sending an error response to the client
     *
     * Clears any state set in SamoaResponse, and arranges for
     *  an error response to be delieved to the client.
     *
     * Postcondition: same requirements as flush_client_response()
     *
     * @param err_code Code to set on response error
     * @param err_msg Accompanying message
     */
    void send_client_error(unsigned err_code, const std::string & err_msg);

    /*!
     * \brief Helper for sending an error response to the client
     *
     * Clears any state set in SamoaResponse, and arranges for
     *  an error response to be delivered to the client.
     *
     * Postcondition: same requirements as flush_client_response()
     *
     * @param err_code Code to set on response error
     * @param err_msg boost error code, which will be converted to a message
     */
    void send_client_error(unsigned err_code,
        const boost::system::error_code & err_msg);

private:

    void on_client_response(client_response_interface);

    client_ptr_t _client;

    core::protobuf::SamoaRequest   _samoa_request;
    core::protobuf::SamoaResponse _samoa_response;

    std::vector<core::buffer_regions_t> _request_data_blocks;

    context_ptr_t _context;
    peer_set_ptr_t _peer_set;
    table_ptr_t _table;
    std::string _key;

    local_partition_ptr_t _primary_partition;
    partition_peers_t _partition_peers;

    spb::PersistedRecord _local_record;
    spb::PersistedRecord _remote_record;

    unsigned _client_quorum;
    unsigned _error_count;
    unsigned _success_count;

    bool _flush_response_called;
    core::buffer_ring _w_ring;
    core::const_buffer_regions_t _response_data;

    core::io_service_ptr_t _io_srv;
};

}
}

#include "samoa/server/request_state.impl.hpp"
#endif

