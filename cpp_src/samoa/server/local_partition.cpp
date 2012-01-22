#include "samoa/server/local_partition.hpp"
#include "samoa/server/context.hpp"
#include "samoa/server/table.hpp"
#include "samoa/server/eventual_consistency.hpp"
#include "samoa/persistence/persister.hpp"
#include "samoa/datamodel/clock_util.hpp"
#include "samoa/error.hpp"
#include "samoa/log.hpp"
#include <boost/bind.hpp>

namespace samoa {
namespace server {

local_partition::local_partition(
    const spb::ClusterState::Table::Partition & part,
    uint64_t range_begin, uint64_t range_end,
    const ptr_t & current)
 :  partition(part, range_begin, range_end),
    _author_id(datamodel::clock_util::generate_author_id())
{
    SAMOA_ASSERT(part.ring_layer_size());

    if(current)
    {
        _persister = current->_persister;

        if(current->get_range_begin() == get_range_begin() &&
            current->get_range_end() == get_range_end())
        {
            // safe to preserve author id only if we have identical ranges of
            //  responsibility; otherwise, we open ourselves to the possibility
            //  of writing a new version of an existing key with an existing
            //  author clock, such that we appear to be more recent than that
            //  previously written clock
            _author_id = current->get_author_id();
        }
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

void local_partition::initialize(
    const context::ptr_t & context, const table::ptr_t & table)
{
    // slightly convoluted, but we need the binder to hold a shared-
    //  pointer for lifetime management of the functor.
    // without that requirement, a stack eventual_consistency
    //  could be directly passed-by-value as the callback
    _persister->set_record_upkeep_callback(
        boost::bind(&eventual_consistency::operator(),
            boost::make_shared<eventual_consistency>(context,
                table->get_uuid(), get_uuid(),
                table->get_consistent_prune()), _1));
}

}
}

