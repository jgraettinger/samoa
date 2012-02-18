#ifndef SAMOA_SERVER_REPLICATION_HPP
#define SAMOA_SERVER_REPLICATION_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/client/fwd.hpp"
#include "samoa/client/server.hpp"
#include "samoa/request/request_state.hpp"
#include "samoa/core/murmur_checksummer.hpp"
#include <boost/system/error_code.hpp>
#include <boost/function.hpp>

namespace samoa {
namespace server {

class replication
{
public:

    typedef boost::function<void(
        const boost::system::error_code &, bool)> read_callback_t;

    static void repaired_read(
        const read_callback_t &,
        const request::state_ptr_t &); 

    static void replicated_write(
        const request::state_ptr_t &,
        const core::murmur_checksummer::checksum_t & checksum);

    static void replicated_sync(
        const request::state_ptr_t &,
        const core::murmur_checksummer::checksum_t & checksum,
        const core::murmur_checksummer::checksum_t & alternate);

private:

    static void on_peer_read_request(
        const boost::system::error_code & ec,
        samoa::client::server_request_interface,
        const read_callback_t &,
        const request::state_ptr_t &,
        const partition_ptr_t &);

    static void on_peer_read_response(
        const boost::system::error_code & ec,
        samoa::client::server_response_interface,
        const read_callback_t &,
        const request::state_ptr_t &,
        const partition_ptr_t &);

    static void peer_reads_finished(
        const read_callback_t &,
        const request::state_ptr_t &);

    static void on_peer_write_request(
        const boost::system::error_code & ec,
        samoa::client::server_request_interface,
        const boost::function<void()> &,
        const request::state_ptr_t &,
        const core::murmur_checksummer::checksum_t &,
        const partition_ptr_t &);

    static void on_peer_write_response(
        const boost::system::error_code & ec,
        samoa::client::server_response_interface,
        const boost::function<void()> &,
        const request::state_ptr_t &,
        const core::murmur_checksummer::checksum_t &,
        const partition_ptr_t &);

    static void build_peer_request(
        samoa::client::server_request_interface &,
        const request::state_ptr_t &,
        const core::uuid & peer_uuid);
};

}
}

#endif

