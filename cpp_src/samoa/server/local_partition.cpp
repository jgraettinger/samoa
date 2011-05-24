#include "samoa/server/local_partition.hpp"
#include "samoa/error.hpp"

namespace samoa {
namespace server {

local_partition::local_partition(
    const spb::ClusterState::Table::Partition & part,
    const ptr_t & current)
 :  partition(part)
{}

bool local_partition::merge_partition(
    const spb::ClusterState::Table::Partition & peer,
    spb::ClusterState::Table::Partition & local)
{
    // with the exception of dropping, local_partitions are
    //  only be modified... locally
    SAMOA_ASSERT(local.lamport_ts() >= peer.lamport_ts());
    return false;
}

}
}

