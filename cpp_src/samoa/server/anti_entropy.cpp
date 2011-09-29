
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

void anti_entropy::on_iterate(const record * record)
{
    if(!record)
    {
        end_cycle(interval);
        return;
    }

    table::ptr_t table = _weak_table.lock();
    SAMOA_ASSERT(table);

    uint64_t ring_position = table->ring_position(
        std::string(record.key_begin(), record.key_end()));

    table->route_ring_position(ring_position, peer_set::ptr_t(()),
        _primary_partition, _peer_partitions);

    if(!_primary_partition ||
        _primary_partition->get_uuid() != _partition_uuid)
    {
        // this record needs to be moved off of this persister



    } else
    {
        // (maybe?) replicate to peers
        //
    }



}

}
}

