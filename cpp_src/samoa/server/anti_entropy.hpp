#ifndef SAMOA_SERVER_ANTI_ENTROPY_HPP
#define SAMOA_SERVER_ANTI_ENTROPY_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/persistence/fwd.hpp"
#include "samoa/core/periodic_task.hpp"

namespace samoa {
namespace server {

class anti_entropy :
    public core::periodic_task<anti_entropy>
{
public:

    using core::periodic_task<anti_entropy>::ptr_t;
    using core::periodic_task<anti_entropy>::weak_ptr_t;

    anti_entropy(const table_ptr_t &, const local_partition_ptr_t &);

    void begin_cycle();

protected:

    const table_weak_ptr_t _weak_table;
    const persister_weak_ptr_t _weak_persister;
    const core::uuid _partition_uuid;

    unsigned _ticket;
    spb::PersistedRecord _record;

    // reset at completion of each cycle
    local_partition_ptr_t _primary_partition;
    partition_peers_t _peer_partitions;
};

}
}

#endif
