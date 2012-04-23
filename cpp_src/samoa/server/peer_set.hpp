#ifndef SAMOA_SERVER_PEER_SET_HPP
#define SAMOA_SERVER_PEER_SET_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/client/fwd.hpp"
#include "samoa/client/server_pool.hpp"
#include "samoa/request/fwd.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include <map>

namespace samoa {
namespace server {

namespace spb = samoa::core::protobuf;

class peer_set : public samoa::client::server_pool
{
public:

    typedef peer_set_ptr_t ptr_t;

    peer_set(const spb::ClusterState &, const ptr_t &);

    void initialize(const context_ptr_t &);

    void merge_peer_set(const spb::ClusterState & peer,
        spb::ClusterState & local) const;

    /*!
     * Heuristically selects a 'best' peer from amoung the
     *   request's routed peer partitions.
     *
     * Generally this is the peer with lowest observed latency.
     */
    core::uuid select_best_peer(const request::state_ptr_t &);

    /*! \brief Forwards client request to the peer of select_best_peer()
    */
    void forward_request(request::state_ptr_t);

    /*! \brief Begins a peer discovery operation against all peers
     *  Called on startup, periodically (?), or when a local 
     *   state change must be broadcast to peers
     */
    void begin_peer_discovery();

    /*! \brief Begins a discovery operation against a specific peer
     *  Silently ignored if an operation is in-progress
     *  Called whenever a state discrepancy is detected
     *  (ie, forward_request or replication fails due to route error)
     */
    void begin_peer_discovery(const core::uuid & peer_uuid);

    static void set_discovery_enabled(bool enabled)
    { _discovery_enabled = enabled; }

private:

    static bool _discovery_enabled;

    static void on_forwarded_request(
        boost::system::error_code,
        samoa::client::server_request_interface,
        const request::state_ptr_t &,
        core::uuid);

    static void on_forwarded_response(
        boost::system::error_code,
        samoa::client::server_response_interface,
        const request::state_ptr_t &,
        core::uuid);

    typedef std::map<core::uuid, peer_discovery_ptr_t> discovery_functors_t;
    discovery_functors_t _discovery_functors;
};

}
}

#endif

