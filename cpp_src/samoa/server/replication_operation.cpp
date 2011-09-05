
#include "samoa/server/replication_operation.hpp"
#include "samoa/client/server.hpp"
#include "samoa/server/client.hpp"
#include "samoa/server/peer_set.hpp"
#include "samoa/server/local_partition.hpp"
#include "samoa/server/partition.hpp"
#include "samoa/core/protobuf/fwd.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include "samoa/core/protobuf_helpers.hpp"
#include "samoa/core/proactor.hpp"
#include "samoa/error.hpp"
#include "samoa/log.hpp"
#include <boost/bind.hpp>

namespace samoa {
namespace server {

namespace spb = samoa::core::protobuf;


// private constructor-class for use with make_shared
class replication_operation_priv :
    public replication_operation
{
public:

    replication_operation_priv(
        const table_ptr_t & table,
        table::ring_route && route,
        const std::string & key,
        const spb::PersistedRecord_ptr_t & record,
        const client_ptr_t & client,
        unsigned quorom)
     :  replication_operation(
            table, std::move(route), key, record, client, quorom)
    { }
};

replication_operation::replication_operation(
    const table_ptr_t & table,
    table::ring_route && route,
    const std::string & key,
    const spb::PersistedRecord_ptr_t & record,
    const client_ptr_t & client,
    unsigned quorom)
 :  _table(table),
    _route(route),
    _key(key),
    _record(record),
    _io_srv(core::proactor::get_proactor()->serial_io_service()),
    _client(client),
    _quorom_count(quorom),
    _error_count(0),
    _success_count(0)
{ }

void replication_operation::spawn_replication(
    const peer_set_ptr_t & peer_set,
    const table_ptr_t & table,
    table::ring_route && route,
    const std::string & key,
    const spb::PersistedRecord_ptr_t & record,
    const client_ptr_t & client,
    unsigned quorom)
{
    replication_operation::ptr_t op = \
        boost::make_shared<replication_operation_priv>(
            peer_set, table, std::move(route), key, record, client, quorom);

    // iterate through secondary partitions,
    //  spawning replication requests
    for(auto it = op->_route.secondary_partitions.begin();
        it != op->_route.secondary_partitions.end(); ++it)
    {
        // callbacks are wrapped here by serial _io_srv,
        //  to synchronize potentially concurrent callbacks

        if(it->second)
        {
            // use server instance from the ring_route
            it->second->schedule_request(
                op->_io_srv->wrap(
                    boost::bind(&replication_operation::on_request,
                        op, _1, _2, it->first)));
        }
        else
        {
            // defer to peer_set for server instance
            peer_set->schedule_request(
                op->_io_srv->wrap(
                    boost::bind(&replication_operation::on_request,
                        op, _1, _2, it->first)),
                it->first->get_server_uuid());
        }
    }
}

void replication_operation::spawn_replication(
    const peer_set_ptr_t & peer_set,
    const table_ptr_t & table,
    table::ring_route && route,
    const std::string & key,
    const spb::PersistedRecord_ptr_t & record)
{
    spawn_replication(peer_set, table, std::move(route), key, record, client_ptr_t(), 0);
}

void replication_operation::on_request(
    const boost::system::error_code & ec,
    samoa::client::server_request_interface server,
    const partition_ptr_t & target_partition)
{
    if(ec)
    {
        LOG_ERR(ec.message());
        return;
    }

    server.get_message().set_type(spb::REPLICATE_BLOB);

    spb::ReplicationRequest & repl_request = \
        *server.get_message().mutable_replication();

    repl_request.mutable_table_uuid()->assign(
        _table->get_uuid().begin(), _table->get_uuid().end());

    repl_request.set_key(_key);

    repl_request.mutable_source_partition_uuid()->assign(
        _route.primary_partition->get_uuid().begin(),
        _route.primary_partition->get_uuid().end());

    repl_request.mutable_target_partition_uuid()->assign(
        target_partition->get_uuid().begin(),
        target_partition->get_uuid().end());

    // write record itself
    _record->SerializeToZeroCopyStream(&_zc_adapter);
    server.get_message().add_data_block_length(_zc_adapter.ByteCount());

    server.start_request();
    server.write_interface().queue_write(_zc_adapter.output_regions());

    _zc_adapter.reset();

    // again, synchronize callbacks via serial _io_srv
    server.finish_request(
        _io_srv->wrap(
            boost::bind(&replication_operation::on_response,
                shared_from_this(), _1, _2, target_partition)));
}

void replication_operation::on_response(
    const boost::system::error_code & ec,
    samoa::client::server_response_interface server,
    const partition_ptr_t & target_partition)
{
    if(ec)
    {
        LOG_ERR(ec.message());
        _error_count += 1;
    }
    else
    {
        if(server.get_error_code())
        {
            const spb::Error & error = server.get_message().error();

            LOG_ERR("partition " << target_partition->get_uuid()
                << ": " << error.code() << " " << error.message());

            _error_count += 1;
        }
        else
            _success_count += 1;

        server.finish_response();
    }

    unsigned total_count = _error_count + _success_count;

    if(_client && _success_count == _quorom_count)
    {
        _client->finish_response();
    }
    else if(_client && total_count == _route.secondary_partitions.size())
    {
        _client->send_error(500, "replication quorum failed");
    }
}

}
}

