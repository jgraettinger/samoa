#ifndef SAMOA_SERVER_PEER_SET_HPP
#define SAMOA_SERVER_PEER_SET_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/client/fwd.hpp"
#include "samoa/client/server_pool.hpp"
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

    /*! \brief Forwards the client request to a peer server
    */
    void forward_request(const client_ptr_t &,
        const core::uuid & peer_uuid);

private:

    void on_forwarded_request(const boost::system::error_code &,
        const client_ptr_t &, samoa::client::server_request_interface &);

    void on_forwarded_response(const boost::system::error_code &,
        const client_ptr_t &, samoa::client::server_response_interface &);

    typedef std::map<core::uuid, peer_discovery_ptr_t> discovery_tasklets_t;
    discovery_tasklets_t _discovery_tasklets;
};

}
}

#endif

