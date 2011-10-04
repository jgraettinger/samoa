#ifndef SAMOA_SERVER_STATE_ROUTE_STATE_HPP
#define SAMOA_SERVER_STATE_ROUTE_STATE_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/core/uuid.hpp"
#include <vector>
#include <string>

namespace samoa {
namespace server {
namespace state {

class route_state
{
public:

    route_state();

    bool has_key() const
    { return !_key.empty(); }

    const std::string & get_key() const
    { return _key; }

    uint64_t get_ring_position() const
    { return _ring_position; }

    bool has_primary_partition_uuid() const
    { return _primary_partition_uuid.is_nil(); }

    const core::uuid & get_primary_partition_uuid() const
    { return _primary_partition_uuid; }

    bool has_peer_partition_uuids() const
    { return !_peer_partition_uuids.empty(); }

    const std::vector<core::uuid> get_peer_partition_uuids() const
    { return _peer_partition_uuids; }

    const local_partition_ptr_t & get_primary_partition() const
    { return _primary_partition; }

    const partition_peers_t & get_partition_peers() const
    { return _partition_peers; }

    void load_route_state(const table_ptr_t &, const peer_set_ptr_t &);

    void reset_route_state();

private:

    std::string _key;
    uint64_t _ring_position;

    core::uuid _primary_partition_uuid;
    std::vector<core::uuid> _peer_partition_uuids;

    local_partition_ptr_t _primary_partition;
    partition_peers_t _partition_peers;
};

}
}
}

#endif

