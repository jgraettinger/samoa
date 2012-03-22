
#include <boost/python.hpp>
#include "samoa/server/remote_partition.hpp"
#include "samoa/server/partition.hpp"

namespace samoa {
namespace server {

namespace bpl = boost::python;

void make_remote_partition_bindings()
{
    bpl::class_<remote_partition, remote_partition::ptr_t,
        bpl::bases<partition>, boost::noncopyable>(
            "RemotePartition", bpl::init<
                const spb::ClusterState::Table::Partition &,
                uint64_t, uint64_t,
                const remote_partition &>())
        .def(bpl::init<const spb::ClusterState::Table::Partition &,
            uint64_t, uint64_t>());

    bpl::implicitly_convertible<remote_partition::ptr_t, partition::ptr_t>();
}

}
}
