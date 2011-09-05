#ifndef SAMOA_SERVER_REPLICATION_OPERATION_HPP
#define SAMOA_SERVER_REPLICATION_OPERATION_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/server/table.hpp"
#include "samoa/core/fwd.hpp"
#include "samoa/core/protobuf/fwd.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include "samoa/core/protobuf_helpers.hpp"
#include <boost/smart_ptr/enable_shared_from_this.hpp>
#include <boost/shared_ptr.hpp>

namespace samoa {
namespace server {

namespace spb = samoa::core::protobuf;

class replication_operation :
    public boost::enable_shared_from_this<replication_operation>
{
public:

    typedef boost::shared_ptr<replication_operation> ptr_t;

    static void spawn_replication(
        const peer_set_ptr_t &,
        const table_ptr_t &,
        table::ring_route &&,
        const std::string & key,
        const spb::PersistedRecord_ptr_t &);

    static void spawn_replication(
        const peer_set_ptr_t &,
        const table_ptr_t &,
        table::ring_route &&,
        const std::string & key,
        const spb::PersistedRecord_ptr_t &,
        const client_ptr_t &,
        unsigned write_quorom);

private:

    replication_operation(
        const table_ptr_t &,
        table::ring_route &&,
        const std::string & key,
        const spb::PersistedRecord_ptr_t &,
        const client_ptr_t &,
        unsigned write_quorom);

    void on_request(const boost::system::error_code &,
        samoa::client::server_request_interface,
        const partition_ptr_t & target_partition);

    void on_response(const boost::system::error_code &,
        samoa::client::server_response_interface,
        const partition_ptr_t & target_partition);

    table::ptr_t _table;
    table::ring_route _route;
    std::string _key;
    spb::PersistedRecord_ptr_t _record;

    core::zero_copy_output_adapter _zc_adapter;
    
    // replication_operation's callbacks are synchronized
    //  by wrapping on a serial io-service
    core::io_service_ptr_t _io_srv;

    // optionally notify a client of quroum success / failure
    client_ptr_t _client;

    unsigned _quorom_count;
    unsigned _error_count;
    unsigned _success_count;

    friend class replication_operation_priv;
};

}
}

#endif

