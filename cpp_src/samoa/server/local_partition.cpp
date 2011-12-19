#include "samoa/server/local_partition.hpp"
#include "samoa/server/context.hpp"
#include "samoa/server/table.hpp"
#include "samoa/server/eventual_consistency.hpp"
#include "samoa/persistence/persister.hpp"
#include "samoa/error.hpp"
#include "samoa/log.hpp"

namespace samoa {
namespace server {

local_partition::local_partition(
    const spb::ClusterState::Table::Partition & part,
    uint64_t range_begin, uint64_t range_end,
    const ptr_t & current)
 :  partition(part, range_begin, range_end)
{
    SAMOA_ASSERT(part.ring_layer_size());

    if(current)
    {
        _persister = current->_persister;
    }
    else
    {
        _persister.reset(new persistence::persister());

        LOG_DBG("local_partition " << part.uuid() \
            << " built persister " << _persister.get());

        for(auto it = part.ring_layer().begin();
            it != part.ring_layer().end(); ++it)
        {
            if(it->has_file_path())
            {
                _persister->add_mapped_hash_ring(
                    it->file_path(), it->storage_size(), it->index_size());
            }
            else
            {
                _persister->add_heap_hash_ring(
                    it->storage_size(), it->index_size());
            }
        }
    }
}

bool local_partition::merge_partition(
    const spb::ClusterState::Table::Partition & peer,
    spb::ClusterState::Table::Partition & local) const
{
    // with the exception of being dropped, local_partitions are
    //  only to be modified... locally
    SAMOA_ASSERT(local.lamport_ts() >= peer.lamport_ts());
    return false;
}

void local_partition::spawn_tasklets(
    const context::ptr_t & context, const table::ptr_t & table)
{
    _persister->set_record_upkeep_callback(
        eventual_consistency(context,
            table->get_uuid(), get_uuid(),
            table->get_consistent_prune()));
}

}
}

