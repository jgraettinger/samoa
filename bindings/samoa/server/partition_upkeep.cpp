
#include <boost/python.hpp>
#include "samoa/server/partition_upkeep.hpp"
#include "samoa/server/context.hpp"
#include "samoa/server/table.hpp"
#include "samoa/server/local_partition.hpp"

namespace samoa {
namespace server {

namespace bpl = boost::python;

void make_partition_upkeep_bindings()
{
    /*
    bpl::class_<partition_upkeep, partition_upkeep::ptr_t, boost::noncopyable>(
        "PartitionUpkeep", bpl::init<const context::ptr_t, const table::ptr_t,
            const local_partition::ptr_t>())
        ;
    */
}

}
}

