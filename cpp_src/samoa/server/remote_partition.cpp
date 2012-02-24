#include "samoa/server/remote_partition.hpp"
#include "samoa/server/remote_digest.hpp"
#include "samoa/error.hpp"

namespace samoa {
namespace server {

remote_partition::remote_partition(
    const spb::ClusterState::Table::Partition & part,
    uint64_t range_begin, uint64_t range_end,
    const remote_partition::ptr_t & current)
 :  partition(part, range_begin, range_end)
{
	if(current)
    {
    	_digest = current->get_digest();
    }
    else
    {
        _digest = boost::make_shared<remote_digest>(get_uuid());
    }
}

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

void remote_partition::initialize(
    const context_ptr_t &, const table_ptr_t &)
{ }

}
}

