#include "samoa/server/local_partition.hpp"
#include "samoa/persistence/persister.hpp"
#include "samoa/error.hpp"
#include "samoa/log.hpp"

namespace samoa {
namespace server {

local_partition::local_partition(
    const spb::ClusterState::Table::Partition & part,
    const ptr_t & current)
 :  partition(part)
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
                _persister->add_mapped_hash(
                    it->file_path(), it->storage_size(), it->index_size());
            }
            else
            {
                _persister->add_heap_hash(
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

}
}

