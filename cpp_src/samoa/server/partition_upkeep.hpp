#ifndef SAMOA_SERVER_PARTITION_UPKEEP_HPP
#define SAMOA_SERVER_PARTITION_UPKEEP_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/persistence/fwd.hpp"
#include "samoa/core/periodic_task.hpp"
#include "samoa/core/uuid.hpp"

namespace samoa {
namespace server {

class partition_upkeep :
    public core::periodic_task<partition_upkeep>
{
public:

    using core::periodic_task<partition_upkeep>::ptr_t;
    using core::periodic_task<partition_upkeep>::weak_ptr_t;

    partition_upkeep(const context_ptr_t &, const table_ptr_t &,
        const local_partition_ptr_t &);

    void begin_cycle();

protected:

    void on_iterate(const persistence::persister_ptr_t,
        const record * raw_record);

    request::state_ptr_t load_raw_record(const persistence::record *);


    const context_ptr_t _weak_context;
    const persister_weak_ptr_t _weak_persister;

    const core::uuid _table_uuid;
    const core::uuid _partition_uuid;

    unsigned _ticket;
};

}
}

#endif

