
#include "samoa/request/record_state.hpp"

namespace samoa {
namespace request {

record_state::record_state()
{
    reset_record_state();
}

record_state::~record_state()
{ }

void record_state::reset_record_state()
{
    _local_record.Clear();
    _remote_record.Clear();

    _local_record.mutable_cluster_clock(
        )->set_clock_is_pruned(false);
}

}
}

