
#include "samoa/server/replication.hpp"
#include "samoa/server/peer_set.hpp"
#include "samoa/server/local_partition.hpp"
#include "samoa/server/partition.hpp"
#include "samoa/server/table.hpp"
#include "samoa/request/request_state.hpp"
#include "samoa/error.hpp"
#include "samoa/log.hpp"
#include <boost/bind.hpp>

namespace samoa {
namespace server {

void replication::replicated_read(
    const replication::callback_t & callback,
    const request::state::ptr_t & rstate)
{
    replicated_op(callback, rstate, false);
}

void replication::replicated_write(
    const replication::callback_t & callback,
    const request::state::ptr_t & rstate)
{
    replicated_op(callback, rstate, true);
}

void replication::replicated_op(
    const replication::callback_t & callback,
    const request::state::ptr_t & rstate,
    bool write_request)
{
    request::route_state & route_state = rstate->get_route_state();

    for(auto it = route_state.get_peer_partitions().begin();
            it != route_state.get_peer_partitions().end(); ++it)
    {
        rstate->get_peer_set()->schedule_request(
            // wrap with request's io-service to synchronize callbacks
            rstate->get_io_service()->wrap(
                boost::bind(&replication::on_peer_request,
                    _1, _2, callback, rstate, *it, write_request)),
            (*it)->get_server_uuid());
    }
}

void replication::on_peer_request(
    const boost::system::error_code & ec,
    samoa::client::server_request_interface iface,
    const replication::callback_t & callback,
    const request::state::ptr_t & rstate,
    const partition::ptr_t & peer_part,
    bool write_request)
{
    request::route_state & route_state = rstate->get_route_state();
    request::replication_state & repl_state = rstate->get_replication_state();
    request::record_state & record_state = rstate->get_record_state();

    if(ec)
    {
        LOG_WARN(ec.message());

        if(repl_state.peer_replication_failure())
        {
            callback();
        }
        return;
    }

    if(!write_request && repl_state.is_client_quorum_met())
    {
        // this read-replication request no longer needs to be made
        iface.abort_request();
        return;
    }

    spb::SamoaRequest & samoa_request = iface.get_message();

    samoa_request.set_type(spb::REPLICATE);
    samoa_request.set_key(route_state.get_key());

    samoa_request.mutable_table_uuid()->assign(
        rstate->get_table_uuid().begin(),
        rstate->get_table_uuid().end());

    // assign remote partition as primary partition_uuid
    samoa_request.mutable_partition_uuid()->assign(
        peer_part->get_uuid().begin(),
        peer_part->get_uuid().end());

    // assign our local partition as a peer_partition_uuid
    samoa_request.add_peer_partition_uuid()->assign(
        route_state.get_primary_partition_uuid().begin(),
        route_state.get_primary_partition_uuid().end());

    // assign _other_ remote partitions as peer_partition_uuid
    for(auto it = route_state.get_peer_partition_uuids().begin();
            it != route_state.get_peer_partition_uuids().end(); ++it)
    {
        if(*it != peer_part->get_uuid())
        {
            samoa_request.add_peer_partition_uuid()->assign(
                it->begin(), it->end());
        }
    }

    // if this is a write-replication, send local-record to peer
    if(write_request)
    {
        core::zero_copy_output_adapter zco_adapter;

        record_state.get_local_record().SerializeToZeroCopyStream(&zco_adapter);
        iface.add_data_block(zco_adapter.output_regions());
    }

    iface.flush_request(
        rstate->get_io_service()->wrap(
            // wrap with request's io-service to synchronize callbacks
            boost::bind(&replication::on_peer_response,
                _1, _2, callback, rstate, write_request)));
}

void replication::on_peer_response(
    const boost::system::error_code & ec,
    samoa::client::server_response_interface iface,
    const replication::callback_t & callback,
    const request::state::ptr_t & rstate,
    bool write_request)
{
    request::replication_state & repl_state = rstate->get_replication_state();
    request::record_state & record_state = rstate->get_record_state();

    if(ec || iface.get_error_code())
    {
        if(ec)
        {
            LOG_WARN(ec.message());
        }
        else
        {
            LOG_WARN("remote error: " << \
                iface.get_message().error().ShortDebugString());

            iface.finish_response();
        }

        if(repl_state.peer_replication_failure())
        {
            callback();
        }
        return;
    }

    // is this a non-empty response to a still-needed replicated read?
    if(!write_request &&
       !repl_state.is_client_quorum_met() &&
       !iface.get_response_data_blocks().empty())
    {
        // parse into local-record (used here as scratch space)
        SAMOA_ASSERT(iface.get_response_data_blocks().size() == 1);

        core::zero_copy_input_adapter zci_adapter(
            iface.get_response_data_blocks()[0]);

        SAMOA_ASSERT(record_state.get_local_record().ParseFromZeroCopyStream(
            &zci_adapter));

        // merge local-record into remote-record
        rstate->get_table()->get_consistent_merge()(
            record_state.get_remote_record(), record_state.get_local_record());
    }

    iface.finish_response();

    if(repl_state.peer_replication_success())
    {
        callback();
    }
}

}
}

