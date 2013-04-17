#ifndef SAMOA_SERVER_PEER_DISCOVERY_HPP
#define SAMOA_SERVER_PEER_DISCOVERY_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/client/fwd.hpp"
#include "samoa/core/uuid.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include "samoa/spinlock.hpp"
#include <boost/asio.hpp>
#include <functional>
#include <memory>

namespace samoa {
namespace server {

class peer_discovery :
    public std::enable_shared_from_this<peer_discovery>
{
public:

    typedef peer_discovery_ptr_t ptr_t;

    peer_discovery(const context_ptr_t &, const core::uuid & peer_uuid);

    typedef std::function<void(const boost::system::error_code &
        )> callback_t;

    void operator()();

    // if in progress, calls back immediately with in-progress
    void operator()(callback_t &&); 

protected:

    void on_request(boost::system::error_code,
        samoa::client::server_request_interface);

    void on_response(boost::system::error_code,
        samoa::client::server_response_interface);

    bool on_state_transaction(core::protobuf::ClusterState &);

    void finish(const boost::system::error_code &);

    const context_weak_ptr_t _weak_context;
    const core::uuid _peer_uuid;

    // serializes concurrent calls
    spinlock _lock;

    callback_t _callback;
    context_ptr_t _context;
    core::protobuf::ClusterState _remote_state;
};

}
}

#endif

