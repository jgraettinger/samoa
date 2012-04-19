#ifndef SAMOA_SERVER_REPLICATION_HPP
#define SAMOA_SERVER_REPLICATION_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/client/fwd.hpp"
#include "samoa/client/server.hpp"
#include "samoa/request/request_state.hpp"
#include "samoa/core/fwd.hpp"
#include <boost/system/error_code.hpp>
#include <functional>

namespace samoa {
namespace server {

class replication
{
public:

    typedef std::function<bool(
        samoa::client::server_request_interface &,
        const partition_ptr_t &)
    > peer_request_callback_t;

    typedef std::function<void(
        const boost::system::error_code &,
        samoa::client::server_response_interface &,
        const partition_ptr_t &)
    > peer_response_callback_t;

    static void replicate(
        const peer_request_callback_t &,
        const peer_response_callback_t &,
        const request::state_ptr_t &);

private:

    static void on_request(
        const boost::system::error_code &,
        samoa::client::server_request_interface,
        const peer_request_callback_t &,
        const peer_response_callback_t &,
        const request::state_ptr_t &,
        const partition_ptr_t &);

    static void on_response(
        const boost::system::error_code & ec,
        samoa::client::server_response_interface,
        const peer_response_callback_t &,
        const request::state_ptr_t &,
        const partition_ptr_t &);

    static void build_peer_request(
        samoa::client::server_request_interface &,
        const request::state_ptr_t &,
        const core::uuid & peer_uuid);
};

}
}

#endif

