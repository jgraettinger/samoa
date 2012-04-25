
#include <boost/python.hpp>
#include "samoa/server/partition.hpp"
#include "samoa/server/digest.hpp"

namespace samoa {
namespace server {

namespace bpl = boost::python;

void make_partition_bindings()
{
    bpl::class_<partition, partition::ptr_t, boost::noncopyable>(
            "Partition", bpl::no_init)
        .def("get_uuid", &partition::get_uuid,
            bpl::return_value_policy<bpl::copy_const_reference>())
        .def("get_server_uuid", &partition::get_server_uuid,
            bpl::return_value_policy<bpl::copy_const_reference>())
        .def("get_ring_position", &partition::get_ring_position)
        .def("get_range_begin", &partition::get_range_begin)
        .def("get_range_end", &partition::get_range_end)
        .def("get_consistent_range_begin",
            &partition::get_consistent_range_begin)
        .def("get_consistent_range_end",
            &partition::get_consistent_range_end)
        .def("get_lamport_ts", &partition::get_lamport_ts)
        .def("is_tracked", &partition::is_tracked)
        .def("get_digest", &partition::get_digest)
        .def("position_in_responsible_range",
            &partition::position_in_responsible_range)
        .def("merge_partition", &partition::merge_partition);
}

}
}
