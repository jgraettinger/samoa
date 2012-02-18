
#include "samoa/server/partition.hpp"
#include "samoa/server/consistent_set.hpp"
#include "samoa/core/uuid.hpp"

namespace samoa {
namespace server {

partition::partition(
    const spb::ClusterState::Table::Partition & part,
    uint64_t range_begin, uint64_t range_end)
 : _uuid(core::parse_uuid(part.uuid())),
   _server_uuid(core::parse_uuid(part.server_uuid())),
   _ring_position(part.ring_position()),
   _range_begin(range_begin),
   _range_end(range_end),
   _consistent_range_begin(part.consistent_range_begin()),
   _consistent_range_end(part.consistent_range_end()),
   _lamport_ts(part.lamport_ts()),
   _consistent_set(boost::make_shared<consistent_set>())
{}

partition::~partition()
{}

bool partition::position_in_responsible_range(uint64_t pos) const
{
    if(_range_end < _range_begin)
    {
        // effective range wraps around through 0
        return pos <= _range_end || pos >= _range_begin;
    }
    else
    {
        // simple inclusion
        return pos >= _range_begin && pos <= _range_end;
    }
}

}
}

