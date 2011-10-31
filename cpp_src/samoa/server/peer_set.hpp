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

    void spawn_tasklets(const context_ptr_t &);

    void merge_peer_set(const spb::ClusterState & peer,
        spb::ClusterState & local) const;

    /*!
     * Heuristically selects a 'best' peer from amoung the
     *   request's routed peer partitions.
     *
     * Generally this is the peer with lowest observed latency.
     */
    core::uuid select_best_peer(const request::state_ptr_t &);

    /*! \brief Forwards the client request to a peer server
    */
    void forward_request(const request::state_ptr_t &);

private:

    void on_forwarded_request(
        const boost::system::error_code &,
        samoa::client::server_request_interface &,
        const request::state_ptr_t &);

    void on_forwarded_response(
        const boost::system::error_code &,
        samoa::client::server_response_interface &,
        const request::state_ptr_t &);

    typedef std::map<core::uuid, peer_discovery_ptr_t> discovery_tasklets_t;
    discovery_tasklets_t _discovery_tasklets;
};

}
}

#endif

