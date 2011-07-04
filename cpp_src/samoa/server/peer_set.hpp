#ifndef SAMOA_SERVER_PEER_SET_HPP
#define SAMOA_SERVER_PEER_SET_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/client/server_pool.hpp"
#include "samoa/core/protobuf/samoa.pb.h"

namespace samoa {
namespace server {

namespace spb = samoa::core::protobuf;

class peer_set : public samoa::client::server_pool
{
public:

    typedef peer_set_ptr_t ptr_t;

    peer_set(const spb::ClusterState &, const ptr_t &);

    void spawn_tasklets(const core::tasklet_group_ptr_t &);

    void merge_peer_set(const spb::ClusterState & peer,
        spb::ClusterState & local) const;
};

}
}

#endif

