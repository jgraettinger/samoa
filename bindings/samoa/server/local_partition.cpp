
#include <boost/python.hpp>
#include "samoa/server/local_partition.hpp"
#include "samoa/server/partition.hpp"
#include "samoa/persistence/persister.hpp"

namespace samoa {
namespace server {

namespace bpl = boost::python;

void make_local_partition_bindings()
{
    bpl::class_<local_partition, local_partition::ptr_t,
        bpl::bases<partition>, boost::noncopyable>(
            "LocalPartition", bpl::init<
                const spb::ClusterState::Table::Partition &,
                uint64_t, uint64_t,
                const local_partition &>())
        .def(bpl::init<const spb::ClusterState::Table::Partition &,
            uint64_t, uint64_t>())
        .def("get_author_id", &local_partition::get_author_id)
        .def("get_persister", &local_partition::get_persister,
            bpl::return_value_policy<bpl::copy_const_reference>())
        ;

    bpl::implicitly_convertible<local_partition::ptr_t, partition::ptr_t>();
}

}
}
