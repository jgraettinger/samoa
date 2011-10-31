#include "samoa/server/partition_upkeep.hpp"
#include "samoa/server/local_partition.hpp"
#include "samoa/server/table.hpp"
#include "samoa/server/context.hpp"
#include "samoa/server/peer_set.hpp"
#include "samoa/server/replication.hpp"
#include "samoa/persistence/persister.hpp"
#include "samoa/request/request_state.hpp"
#include "samoa/request/state_exception.hpp"
#include "samoa/core/server_time.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include "samoa/error.hpp"
#include "samoa/log.hpp"

namespace samoa {
namespace server {

namespace spb = samoa::core::protobuf;

// default period of 1 ms
boost::posix_time::time_duration partition_upkeep::period = \
    boost::posix_time::milliseconds(1);

partition_upkeep::partition_upkeep(
    const context::ptr_t & context,
    const table::ptr_t & table,
    const local_partition::ptr_t & partition)
 :  core::periodic_task<partition_upkeep>(),
    _weak_context(context),
    _weak_persister(partition->get_persister()),
    _table_uuid(table->get_uuid()),
    _partition_uuid(partition->get_uuid()),
    _ticket(0)
{ }

void partition_upkeep::begin_cycle()
{
    LOG_DBG("called " << get_tasklet_name());

    persistence::persister::ptr_t persister = _weak_persister.lock();
    SAMOA_ASSERT(persister);

    if(!_ticket)
    {
        _ticket = persister->begin_iteration();
        LOG_INFO("beginning new persister iteration round " << _ticket);
    }

    if(!persister->iterate(
        boost::bind(&partition_upkeep::on_iterate,
            shared_from_this(), persister, _1),
        _ticket))
    {
        LOG_INFO("completed iteration of " << _partition_uuid);

        _ticket = 0;
        next_cycle(period);
    }
}

void partition_upkeep::on_iterate(
    const persistence::persister::ptr_t & persister,
    const persistence::record * raw_record)
{
    // Note: we hold an exclusive lock on the persister within this scope!

    // schedule next iteration
    next_cycle(period);

    // build an internal request state for this record
    request::state::ptr_t rstate = boost::make_shared<request::state>();

    rstate->set_key(std::string(raw_record->key_begin(),
        raw_record->key_end()));

    // parse record value into remote record
    SAMOA_ASSERT(rstate->get_remote_record().ParseFromArray(
        raw_record->value_begin(), raw_record->value_length()));

    spb::PersistedRecord & proto_rec = rstate->get_remote_record();

    if(proto_rec.has_expire_timestamp() &&
        proto_rec.expire_timestamp() < core::server_time::get_time())
    {
        // this record has expired; drop it
        persister->drop(
            boost::bind(&partition_upkeep::on_record_expire,
                shared_from_this(), _1, rstate),
            rstate->get_key(), rstate->get_local_record());

        return;
    }

    // callback outside of this scope to release exclusive persister lock
    get_io_service()->post(
        boost::bind(&partition_upkeep::on_record_upkeep,
            shared_from_this(), persister, rstate));
}

bool partition_upkeep::on_record_expire(bool found,
    const request::state::ptr_t & rstate)
{
    SAMOA_ASSERT(found);

    // race condition check: ensure record should still be dropped
    //   (eg, hasn't been re-written since iteration callback)

    spb::PersistedRecord & proto_rec = rstate->get_local_record();

    return proto_rec.has_expire_timestamp() && \
        proto_rec.expire_timestamp() < core::server_time::get_time();
}

void partition_upkeep::on_record_upkeep(
    const persistence::persister::ptr_t & persister,
    const request::state::ptr_t & rstate)
{
    // load remaining request::state for this record
    try {
        load_request_state(rstate);
    }
    catch(const request::state_exception & e)
    {
        LOG_ERR("load_request_state() threw: " << e.what());
        return;
    }

    if(rstate->get_primary_partition_uuid() != _partition_uuid)
    {
        // this record doesn't belong on our partition;
        //   attempt to replicate it to responsible partitions
        rstate->get_peer_set()->schedule_request(
            boost::bind(&partition_upkeep::on_move_request,
                shared_from_this(), _1, _2, persister, rstate),
            rstate->get_peer_set()->select_best_peer(rstate));
    }
    else
    {
        // tentitively re-write the record
        //
        // the table's consistent merge will be atomically invoked,
        //  and given the opportunity to compact the record

        persister->put(
            boost::bind(&partition_upkeep::on_compact,
                    shared_from_this(), _1, _2, rstate),
                datamodel::merge_func_t(
                    rstate->get_table()->get_consistent_merge()),
                rstate->get_key(),
                rstate->get_remote_record(),
                rstate->get_local_record());
    }
}

void partition_upkeep::on_move_request(
    const boost::system::error_code & ec,
    samoa::client::server_request_interface iface,
    const persistence::persister::ptr_t & persister,
    const request::state::ptr_t & rstate)
{
    if(ec)
    {
        LOG_WARN(ec.message());
        return;
    }

    spb::SamoaRequest & samoa_request = iface.get_message();

    samoa_request.set_type(spb::REPLICATE);
    samoa_request.set_key(rstate->get_key());

    // replication fanout should be done by peer
    samoa_request.set_requested_quorum(
        rstate->get_table()->get_replication_factor());

    samoa_request.mutable_table_uuid()->assign(
        rstate->get_table_uuid().begin(),
        rstate->get_table_uuid().end());

    // serialize remote record to the peer
    core::zero_copy_output_adapter zco_adapter;
    SAMOA_ASSERT(rstate->get_remote_record(
        ).SerializeToZeroCopyStream(&zco_adapter));
    iface.add_data_block(zco_adapter.output_regions());

    iface.flush_request(
        boost::bind(&partition_upkeep::on_move_response,
            shared_from_this(), _1, _2, persister, rstate));
}

void partition_upkeep::on_move_response(
    const boost::system::error_code & ec,
    samoa::client::server_response_interface iface,
    const persistence::persister::ptr_t & persister,
    const request::state::ptr_t & rstate)
{
    if(ec || iface.get_error_code())
    {
        if(ec)
        {
            LOG_WARN(ec.message());
        }
        else
        {
            LOG_WARN("remote error on move: " << \
                iface.get_message().error().ShortDebugString());

            iface.finish_response();
        }
        return;
    }

    if(iface.get_message().replication_success() != \
        rstate->get_table()->get_replication_factor())
    {
        LOG_WARN("failed to meet quorum while moving " << rstate->get_key());
    }
    else
    {
        persister->drop(
            boost::bind(&partition_upkeep::on_move_drop,
                shared_from_this(), _1),
            rstate->get_key(), rstate->get_local_record());
    }
}

bool partition_upkeep::on_move_drop(bool found)
{
    SAMOA_ASSERT(found);
    return true;
}

void partition_upkeep::on_compact(
    const boost::system::error_code & ec,
    const datamodel::merge_result & merge_result,
    const request::state::ptr_t & rstate)
{
    if(ec)
    {
        LOG_WARN(ec.message());
        return;
    }

    replication::replicated_write(
        boost::bind(&partition_upkeep::on_replicate_sync,
            shared_from_this()),
        rstate);
}

void partition_upkeep::on_replicate_sync()
{

}

void partition_upkeep::load_request_state(
    const request::state::ptr_t & rstate)
{
    rstate->load_io_service_state(get_io_service());
    rstate->load_context_state(_weak_context.lock());

    rstate->set_table_uuid(_table_uuid);
    rstate->load_table_state();

    // assume the common case, that this key
    //   belongs with this partition

    // anchor record's route with this partition as primary
    rstate->set_primary_partition_uuid(_partition_uuid);

    try {
        rstate->load_route_state();
    }
    catch(const request::state_exception & e)
    {
        std::string key = rstate->get_key();

        // this record doesn't belong on this partition
        rstate->reset_route_state();

        rstate->set_key(std::move(key));

        // determine responsible peers
        rstate->load_route_state();
    }

    // fully replicate to all peers before responding
    rstate->set_quorum_count(0);
    rstate->load_replication_state();

    return;
}

}
}

