
#include <boost/python.hpp>
#include "samoa/server/local_partition.hpp"
#include "samoa/server/partition.hpp"

namespace samoa {
namespace server {

namespace bpl = boost::python;

void make_local_partition_bindings()
{
    bpl::class_<local_partition, local_partition::ptr_t,
        bpl::bases<partition>, boost::noncopyable>(
            "LocalPartition", bpl::init<
                const spb::ClusterState::Table::Partition &,
                const local_partition::ptr_t &>());

    bpl::implicitly_convertible<local_partition::ptr_t, partition::ptr_t>();
}

}
}
