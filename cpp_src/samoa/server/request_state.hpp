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


class request_state : 
    public boost::enable_shared_from_this<request_state>
{
public:

    typedef boost::shared_ptr<request_state> ptr_t;

    request_state(const server::client_ptr_t &);

    bool load_from_samoa_request(const context_ptr_t &);


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

private:

    peer_set_ptr_t _peer_set;
    table_ptr_t _table;
    std::string _key;


    unsigned _client_quorum;
    unsigned _error_count;
    unsigned _success_count;

    core::io_service_ptr_t _io_srv;
};

}
}

#include "samoa/server/request_state.impl.hpp"
#endif

