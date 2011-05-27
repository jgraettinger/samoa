#include "samoa/server/remote_partition.hpp"
#include "samoa/error.hpp"

namespace samoa {
namespace server {

remote_partition::remote_partition(
    const spb::ClusterState::Table::Partition & part,
    const remote_partition::ptr_t & current)
 :  partition(part)
{}

bool remote_partition::merge_partition(
    const spb::ClusterState::Table::Partition & peer,
    spb::ClusterState::Table::Partition & local) const
{
    if(peer.lamport_ts() <= local.lamport_ts())
        return false;

    local.set_consistent_range_begin(peer.consistent_range_begin());
    local.set_consistent_range_end(peer.consistent_range_end());
    local.set_lamport_ts(peer.lamport_ts());
    return true;
}

}
}

