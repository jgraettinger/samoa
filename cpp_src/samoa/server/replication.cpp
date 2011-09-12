
#include "samoa/server/request_state.hpp"
#include "samoa/server/replication.hpp"
#include "samoa/server/peer_set.hpp"
#include "samoa/server/local_partition.hpp"
#include "samoa/server/partition.hpp"
#include "samoa/server/partition_peer.hpp"
#include "samoa/server/table.hpp"
#include "samoa/error.hpp"
#include "samoa/log.hpp"
#include <boost/bind.hpp>

namespace samoa {
namespace server {

void replication::replicated_read(
    const replication::callback_t & callback,
    const request_state::ptr_t & rstate)
{
    replicated_op(callback, rstate, false);
}

void replication::replicated_write(
    const replication::callback_t & callback,
    const request_state::ptr_t & rstate)
{
    replicated_op(callback, rstate, true);
}

void replication::replicated_op(
    const replication::callback_t & callback,
    const request_state::ptr_t & rstate,
    bool write_request)
{
    for(auto p_it = rstate->get_partition_peers().begin();
            p_it != rstate->get_partition_peers().end(); ++p_it)
    {
        const partition_peer & part_peer = *p_it;

        if(part_peer.server)
        {
            // use cached server instance
            part_peer.server->schedule_request(
                rstate->get_io_service()->wrap(
                    boost::bind(&replication::on_peer_request,
                        _1, _2, callback, rstate,
                        part_peer.partition, write_request)));
        }
        else
        {
            // defer to peer_set for server instance
            rstate->get_peer_set()->schedule_request(
                rstate->get_io_service()->wrap(
                    boost::bind(&replication::on_peer_request,
                        _1, _2, callback, rstate,
                        part_peer.partition, write_request)),
                part_peer.partition->get_server_uuid());
        }
    }
}

void replication::on_peer_request(
    const boost::system::error_code & ec,
    samoa::client::server_request_interface server,
    const replication::callback_t & callback,
    const request_state::ptr_t & rstate,
    const partition::ptr_t & partition,
    bool write_request)
{
    if(ec)
    {
        LOG_WARN(ec.message());

        // increments peer_error, and returns true
        //  iff this increment makes it impossible
        //  to meet quorum
        if(rstate->replication_failure())
        {
            callback(ec);
        }
        return;
    }

    if(rstate->replication_complete())
    {
        // this request should be ignored
        server.abort_request();
        return;
    }

    spb::SamoaRequest & samoa_request = server.get_message();

    samoa_request.set_type(spb::REPLICATE);

    samoa_request.set_key(rstate->get_key());

    samoa_request.mutable_table_uuid()->assign(
        rstate->get_table()->get_uuid().begin(),
        rstate->get_table()->get_uuid().end());

    samoa_request.mutable_partition_uuid()->assign(
        partition->get_uuid().begin(),
        partition->get_uuid().end());

    samoa_request.add_peer_partition_uuid()->assign(
        rstate->get_primary_partition()->get_uuid().begin(),
        rstate->get_primary_partition()->get_uuid().end());

    // if this is a replicated write, send local-record to peer
    if(write_request)
    {
        rstate->get_local_record().SerializeToZeroCopyStream(
            &rstate->get_zco_adapter());
        server.get_message().add_data_block_length(
            rstate->get_zco_adapter().ByteCount());

        server.start_request();
        server.write_interface().queue_write(
            rstate->get_zco_adapter().output_regions());

        rstate->get_zco_adapter().reset();
    }

    server.finish_request(
        rstate->get_io_service()->wrap(
            boost::bind(&replication::on_peer_response,
                _1, _2, callback, rstate, write_request)));
}

void replication::on_peer_response(
    const boost::system::error_code & ec,
    samoa::client::server_response_interface server,
    const replication::callback_t & callback,
    const request_state::ptr_t & rstate,
    bool write_request)
{
    if(ec)
    {
        LOG_WARN(ec.message());

        // increments peer_error, and returns true
        //  iff this increment makes it impossible
        //  to meet quorum
        if(rstate->replication_failure())
        {
            callback(ec);
        }
        return;
    }

    if(rstate->replication_complete())
    {
        // this response should be ignored
        server.finish_response();
        return;
    }

    // peer responded with a record for this key?
    if(!write_request && !server.get_response_data_blocks().empty())
    {
        // parse into local-record (used here as scratch space)
        SAMOA_ASSERT(server.get_response_data_blocks().size() == 1);
        rstate->get_zci_adapter().reset(server.get_response_data_blocks()[0]);

        SAMOA_ASSERT(rstate->get_local_record().ParseFromZeroCopyStream(
            &rstate->get_zci_adapter()));

        // merge local-record into remote-record
        rstate->get_table()->get_consistent_merge()(
            rstate->get_local_record(), rstate->get_remote_record());
    }

    // increments peer_success, and returns true
    //  iff this increment caused us to meet quorum
    if(rstate->replication_success())
    {
        callback(boost::system::error_code());
    }
}

}
}

