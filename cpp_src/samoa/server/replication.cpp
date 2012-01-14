
#include "samoa/server/replication.hpp"
#include "samoa/server/peer_set.hpp"
#include "samoa/server/local_partition.hpp"
#include "samoa/server/partition.hpp"
#include "samoa/server/table.hpp"
#include "samoa/persistence/persister.hpp"
#include "samoa/request/request_state.hpp"
#include "samoa/core/protobuf/zero_copy_output_adapter.hpp"
#include "samoa/core/protobuf/zero_copy_input_adapter.hpp"
#include "samoa/error.hpp"
#include "samoa/log.hpp"
#include <boost/bind.hpp>

namespace samoa {
namespace server {

void replication::repaired_read(
    const replication::read_callback_t & callback,
    const request::state::ptr_t & rstate)
{
    // assume the local read will succeed
    if(rstate->peer_replication_success())
    {
        peer_reads_finished(callback, rstate);
    }
    else
    {
        // fanout read replication requests to peers
        for(const partition::ptr_t & partition : rstate->get_peer_partitions())
        {
            rstate->get_peer_set()->schedule_request(
                // wrap with request's io-service to synchronize callbacks
                rstate->get_io_service()->wrap(
                    boost::bind(&replication::on_peer_read_request,
                        _1, _2, callback, rstate,
                        partition->get_server_uuid(), partition->get_uuid())),
                partition->get_server_uuid());
        }
    }
}

void replication::replicated_write(
    const replication::write_callback_t & callback,
    const request::state::ptr_t & rstate)
{
    for(const partition::ptr_t & partition : rstate->get_peer_partitions())
    {
        rstate->get_peer_set()->schedule_request(
            // wrap with request's io-service to synchronize callbacks
            rstate->get_io_service()->wrap(
                boost::bind(&replication::on_peer_write_request,
                    _1, _2, callback, rstate,
                    partition->get_server_uuid(), partition->get_uuid())),
            partition->get_server_uuid());
    }
}

void replication::build_peer_request(
    samoa::client::server_request_interface & iface,
    const request::state::ptr_t & rstate,
    const core::uuid & part_uuid)
{
    spb::SamoaRequest & samoa_request = iface.get_message();

    samoa_request.set_type(spb::REPLICATE);
    samoa_request.set_key(rstate->get_key());

    // we're handling fanout, the peer doesn't need to
    samoa_request.set_requested_quorum(1);

    samoa_request.mutable_table_uuid()->assign(
        std::begin(rstate->get_table_uuid()),
        std::end(rstate->get_table_uuid()));

    // assign remote partition as primary partition_uuid
    samoa_request.mutable_partition_uuid()->assign(
        std::begin(part_uuid), std::end(part_uuid));

    // assign our local partition as a peer_partition_uuid
    samoa_request.add_peer_partition_uuid()->assign(
        std::begin(rstate->get_primary_partition_uuid()),
        std::end(rstate->get_primary_partition_uuid()));

    // assign _other_ remote partitions as peer_partition_uuid
    for(const core::uuid & other_uuid : rstate->get_peer_partition_uuids())
    {
        if(other_uuid != part_uuid)
        {
            samoa_request.add_peer_partition_uuid()->assign(
                std::begin(other_uuid), std::end(other_uuid));
        }
    }
}

void replication::peer_reads_finished(
    const replication::read_callback_t & callback,
    const request::state::ptr_t & rstate)
{
    // did a replicated read return anything?
    if(rstate->had_peer_read_hit())
    {
        // speculatively write the merged remote record; as a side-effect,
        //  local-record will be populated with the merged result
        rstate->get_primary_partition()->get_persister()->put(
            boost::bind(&replication::on_local_read_repair,
                _1, true, callback),
            datamodel::merge_func_t(
                rstate->get_table()->get_consistent_merge()),
            rstate->get_key(),
            rstate->get_remote_record(),
            rstate->get_local_record());
    }
    else
    {
        // no populated peer records exist; fall back to a persister read
        rstate->get_primary_partition()->get_persister()->get(
            boost::bind(&replication::on_local_read_repair,
                boost::system::error_code(), _1, callback),
            rstate->get_key(),
            rstate->get_local_record());
    }
}

void replication::on_peer_read_request(
    const boost::system::error_code & ec,
    samoa::client::server_request_interface iface,
    const replication::read_callback_t & callback,
    const request::state::ptr_t & rstate,
    const core::uuid & peer_uuid,
    const core::uuid & part_uuid)
{
    if(ec)
    {
        LOG_WARN(ec.message());

        if(rstate->peer_replication_failure())
        {
            peer_reads_finished(callback, rstate);
        }
        return;
    }

    if(rstate->is_replication_finished())
    {
        // this peer request no longer needs to be made
        iface.abort_request();
        return;
    }

    build_peer_request(iface, rstate, part_uuid);

    iface.flush_request(
        rstate->get_io_service()->wrap(
            // wrap with request's io-service to synchronize callbacks
            boost::bind(&replication::on_peer_read_response,
                _1, _2, callback, rstate, peer_uuid)));
}

void replication::on_peer_read_response(
    const boost::system::error_code & ec,
    samoa::client::server_response_interface iface,
    const replication::read_callback_t & callback,
    const request::state::ptr_t & rstate,
    const core::uuid & peer_uuid)
{
    if(ec || iface.get_error_code())
    {
        if(ec)
        {
            LOG_WARN(ec.message());
        }
        else
        {
            LOG_WARN("remote error on replicated read: " << \
                iface.get_message().error().ShortDebugString());

            if(iface.get_error_code() == 404)
            {
                // cluster-state may be inconsistent
                rstate->get_peer_set()->begin_peer_discovery(peer_uuid);
            }
            iface.finish_response();
        }

        if(rstate->peer_replication_failure())
        {
            peer_reads_finished(callback, rstate);
        }
        return;
    }

    // is this a still-needed, non-empty response?
    if(!rstate->is_replication_finished() &&
       !iface.get_response_data_blocks().empty())
    {
        // parse into local-record (used here as scratch space)
        SAMOA_ASSERT(iface.get_response_data_blocks().size() == 1);

        core::protobuf::zero_copy_input_adapter zci_adapter(
            iface.get_response_data_blocks()[0]);

        SAMOA_ASSERT(rstate->get_local_record().ParseFromZeroCopyStream(
            &zci_adapter));

        // merge local-record into remote-record
        rstate->get_table()->get_consistent_merge()(
            rstate->get_remote_record(), rstate->get_local_record());

        // mark that a peer read hit
        rstate->set_peer_read_hit();
    }

    iface.finish_response();

    if(rstate->peer_replication_success())
    {
        peer_reads_finished(callback, rstate);
    }
}

void replication::on_local_read_repair(
    const boost::system::error_code & ec,
    bool found,
    const replication::read_callback_t & callback)
{
    callback(ec, found);
}

void replication::on_peer_write_request(
    const boost::system::error_code & ec,
    samoa::client::server_request_interface iface,
    const replication::write_callback_t & callback,
    const request::state::ptr_t & rstate,
    const core::uuid & peer_uuid,
    const core::uuid & part_uuid)
{
    if(ec)
    {
        LOG_WARN(ec.message());

        if(rstate->peer_replication_failure())
        {
            callback();
        }
        return;
    }

    build_peer_request(iface, rstate, part_uuid);

    // serialize local record to the peer
    core::protobuf::zero_copy_output_adapter zco_adapter;
    SAMOA_ASSERT(rstate->get_local_record(
        ).SerializeToZeroCopyStream(&zco_adapter));
    iface.add_data_block(zco_adapter.output_regions());

    iface.flush_request(
        rstate->get_io_service()->wrap(
            // wrap with request's io-service to synchronize callbacks
            boost::bind(&replication::on_peer_write_response,
                _1, _2, callback, rstate, peer_uuid)));
}

void replication::on_peer_write_response(
    const boost::system::error_code & ec,
    samoa::client::server_response_interface iface,
    const replication::write_callback_t & callback,
    const request::state::ptr_t & rstate,
    const core::uuid & peer_uuid)
{
    if(ec || iface.get_error_code())
    {
        if(ec)
        {
            LOG_WARN(ec.message());
        }
        else
        {
            LOG_WARN("remote error on replicated write: " << \
                iface.get_message().error().ShortDebugString());

            if(iface.get_error_code() == 404)
            {
                // cluster-state may be inconsistent
                rstate->get_peer_set()->begin_peer_discovery(peer_uuid);
            }
            iface.finish_response();
        }

        if(rstate->peer_replication_failure())
        {
            callback();
        }
        return;
    }

    iface.finish_response();

    if(rstate->peer_replication_success())
    {
        callback();
    }
}


}
}

