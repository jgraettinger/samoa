#ifndef SAMOA_SERVER_REPLICATION_HPP
#define SAMOA_SERVER_REPLICATION_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/client/fwd.hpp"
#include "samoa/client/server.hpp"
#include "samoa/request/fwd.hpp"
#include "samoa/core/fwd.hpp"
#include "samoa/core/uuid.hpp"
#include <boost/system/error_code.hpp>
#include <memory>
#include <functional>

namespace samoa {
namespace server {

class replication
    : public std::enable_shared_from_this<replication>
{
public:

    // interface may be used but not moved during callback
    typedef std::function<bool(
        const samoa::client::server_request_interface &,
        const partition_ptr_t &)
    > peer_request_callback_t;

    // interface may be used but not moved during callback
    typedef std::function<void(
        boost::system::error_code,
        const samoa::client::server_response_interface &,
        const partition_ptr_t &)
    > peer_response_callback_t;

    static void replicate(
        peer_request_callback_t,
        peer_response_callback_t,
        request::state_ptr_t);

    // exposed for benefit of make_shared; don't call directly
    replication(
        peer_request_callback_t,
        peer_response_callback_t,
        request::state_ptr_t);

private:

    typedef std::shared_ptr<replication> ptr_t;

    static void on_request(
        ptr_t self,
        boost::system::error_code,
        samoa::client::server_request_interface,
        partition_ptr_t);

    static void on_response(
        ptr_t self,
        boost::system::error_code,
        samoa::client::server_response_interface,
        partition_ptr_t);

    void build_peer_request(
        const samoa::client::server_request_interface &,
        const core::uuid &);

    peer_request_callback_t _request_callback;
    peer_response_callback_t _response_callback;
    request::state_ptr_t _rstate;
};

}
}

#endif

