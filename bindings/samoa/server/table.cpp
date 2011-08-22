
#include <boost/python.hpp>
#include "samoa/server/fwd.hpp"
#include "samoa/server/table.hpp"
#include "samoa/server/partition.hpp"
#include "samoa/server/peer_set.hpp"
#include "pysamoa/iterutil.hpp"

namespace samoa {
namespace server {

namespace bpl = boost::python;

bpl::list py_get_ring(const table & t)
{
    const table::ring_t & r = t.get_ring();

    bpl::list l;
    for(auto it = r.begin(); it != r.end(); ++it)
    {
        l.append(*it);
    }
    return l;
}

bpl::tuple py_route_ring_position(const table & t,
    uint64_t ring_position,
    const peer_set::ptr_t & peer_set)
{
    partition::ptr_t primary_partition;
    table::ring_t all_partitions;

    bool is_local = t.route_ring_position(ring_position, peer_set,
        primary_partition, all_partitions);

    bpl::list py_all_partitions;
    for(auto it = all_partitions.begin(); it != all_partitions.end(); ++it)
        py_all_partitions.append(*it);

    return bpl::make_tuple(is_local, primary_partition, py_all_partitions);
}

void make_table_bindings()
{
    bpl::class_<table, table::ptr_t, boost::noncopyable>(
            "Table", bpl::init<const spb::ClusterState::Table &,
                const core::uuid &,
                const table::ptr_t &>())
        .def("get_uuid", &table::get_uuid,
            bpl::return_value_policy<bpl::copy_const_reference>())
        .def("get_name", &table::get_name,
            bpl::return_value_policy<bpl::copy_const_reference>())
        .def("get_data_type", &table::get_data_type)
        .def("get_replication_factor", &table::get_replication_factor)
        .def("get_ring", &py_get_ring)
        .def("get_partition", &table::get_partition)
        .def("ring_position", &table::ring_position)
        .def("route_ring_position", &py_route_ring_position)
        .def("merge_table", &table::merge_table);
}

}
}