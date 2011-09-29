#ifndef SAMOA_SERVER_PEER_DISCOVERY_HPP
#define SAMOA_SERVER_PEER_DISCOVERY_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/client/fwd.hpp"
#include "samoa/core/fwd.hpp"
#include "samoa/core/periodic_task.hpp"
#include "samoa/core/tasklet.hpp"
#include "samoa/core/uuid.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include <boost/asio.hpp>

namespace samoa {
namespace server {

class peer_discovery :
    public core::periodic_task<peer_discovery>
{
public:

    using core::periodic_task<peer_discovery>::ptr_t;
    using core::periodic_task<peer_discovery>::weak_ptr_t;

    peer_discovery(const context_ptr_t &, const core::uuid & peer_uuid);

    void begin_cycle();

protected:

    void on_request(const boost::system::error_code &,
        samoa::client::server_request_interface &,
        const context_ptr_t &);

    void on_response(const boost::system::error_code &,
        samoa::client::server_response_interface &,
        const context_ptr_t &);

    bool on_state_transaction(core::protobuf::ClusterState &,
        const context_ptr_t &);

    context_weak_ptr_t _weak_context;
    const core::uuid _peer_uuid;
    core::protobuf::ClusterState _remote_state;
};

}
}

#endif

