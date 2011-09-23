#ifndef SAMOA_SERVER_REPLICATION_HPP
#define SAMOA_SERVER_REPLICATION_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/client/fwd.hpp"
#include "samoa/client/server.hpp"
#include <boost/system/error_code.hpp>
#include <boost/function.hpp>

namespace samoa {
namespace server {

class replication :
    public boost::enable_shared_from_this<replication>
{
public:

    typedef boost::shared_ptr<replication> ptr_t;

    typedef boost::function<void()> callback_t;

    static void replicated_read(
        const callback_t &,
        const request_state_ptr_t &); 

    static void replicated_write(
        const callback_t &,
        const request_state_ptr_t &);

private:

    static void replicated_op(
        const callback_t &,
        const request_state_ptr_t &,
        bool write_request);

    static void on_peer_request(
        const boost::system::error_code & ec,
        samoa::client::server_request_interface server,
        const callback_t &,
        const request_state_ptr_t &,
        const partition_ptr_t &,
        bool write_request);

    static void on_peer_response(
        const boost::system::error_code & ec,
        samoa::client::server_response_interface server,
        const callback_t &,
        const request_state_ptr_t &,
        bool write_request);
};

}
}

#endif

