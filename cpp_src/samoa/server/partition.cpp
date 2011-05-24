
#include "samoa/server/partition.hpp"
#include "samoa/core/uuid.hpp"

namespace samoa {
namespace server {

partition::partition(
    const spb::ClusterState::Table::Partition & part)
 : _uuid(core::uuid_from_hex(part.uuid())),
   _server_uuid(core::uuid_from_hex(part.server_uuid())),
   _ring_position(part.ring_position()),
   _consistent_range_begin(part.consistent_range_begin()),
   _consistent_range_end(part.consistent_range_end()),
   _lamport_ts(part.lamport_ts())
{}

partition::~partition()
{}

}
}

