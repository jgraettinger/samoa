/*
#include "samoa/server/anti_entropy.hpp"
#include "samoa/server/local_partition.hpp"
#include "samoa/persistence/persister.hpp"
#include "samoa/core/protobuf/samoa.pb.h"

namespace samoa {
namespace server {

anti_entropy::anti_entropy(const table::ptr_t & table,
    const local_partition::ptr_t & partition)
 :  core::periodic_task<anti_entropy>(),
    _weak_table(table),
    _weak_persister(partition->get_persister()),
    _partition_uuid(partition->get_uuid())
    _ticket(0)
{ }

void anti_entropy::begin_cycle()
{
    LOG_DBG("called " << get_tasklet_name());

    persister::ptr_t persister = _weak_persister.lock();
    SAMOA_ASSERT(persister);

    if(!_ticket)
    {
        _ticket = persister->begin_iteration();
        LOG_INFO("beginning new persister iteration round " << _ticket);
    }

    persister->iterate(
        boost::bind(&anti_entropy::on_iterate,
            shared_from_this(), _1),
        ticket);
}

void anti_entropy::on_iterate(const record * raw_record)
{
    if(!record)
    {
        end_cycle(interval);
        return;
    }

    context::ptr_t context = _weak_context.lock();
    SAMOA_ASSERT(context);

    // build an internal request for this table & record
    request_state::ptr_t rstate = boost::make_shared<request_state>(
        client::ptr_t(()));

    spb::SamoaRequest & samoa_request = rstate->get_samoa_request();
    spb::PersistedRecord & record = rstate->get_local_record();

    samoa_request.mutable_table_uuid().assign(
        _table_uuid.begin(), _table_uuid.end());
    samoa_request.mutable_partition_uuid().assign(
        _partition_uuid.begin(), _partition_uuid.end());

    samoa_request.mutable_key().assign(
        record->key_begin(), record->key_end());

    // set to callback only after replication has fully completed
    samoa_request.set_requested_quorum(0);

    // TODO: this is awful; rework it
    try {
        rstate->load_from_samoa_request(context);
    }
    catch(const std::runtime_error & e)
    {
        // partition_uuid isn't responsible for this key;
        //   back off and let the table route
        samoa_request.clear_partition_uuid();
        rstate->load_from_samoa_request(context);
    }

    // parse the iterated record
    SAMOA_ASSERT(record.ParseFromArray(
        raw_record->value_begin(), raw_record->value_length()));

    if(!rstate->get_primary_partition())
    {
        // this key has moved off of this server entirely;
        //   forward replicated-write to the best available peer
        replication::forwarded_write(
            boost::bind(&anti_entropy::on_moved, shared_from_this(), rstate),
            rstate, rstate->get_partition_peers().first().partition);
    }
    else if(rstate->get_primary_partition()->get_uuid() != _partition_uuid)
    {
        // this key has moved to another primary partition on this server;
        //   forward replicated-write to the new primary partition
        replication::forwarded_write(
            boost::bind(&anti_entropy::on_moved, shared_from_this(), rstate),
            rstate, rstate->get_primary_partition());
    }


}

}
}
*/
