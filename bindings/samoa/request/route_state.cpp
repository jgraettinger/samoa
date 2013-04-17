#include "pysamoa/boost_python.hpp"
#include "samoa/request/route_state.hpp"
#include "samoa/server/local_partition.hpp"
#include "samoa/server/partition.hpp"

namespace samoa {
namespace request {

namespace bpl = boost::python;

void py_set_key(route_state & r, const std::string & key)
{
    r.set_key(std::string(key));
}

bpl::list py_get_peer_partition_uuids(route_state & r)
{
    bpl::list out;

    for(auto it = r.get_peer_partition_uuids().begin();
        it != r.get_peer_partition_uuids().end(); ++it)
    {
        out.append(*it);
    }
    return out;
}

bpl::list py_get_peer_partitions(route_state & r)
{
    bpl::list out;

    for(auto it = r.get_peer_partitions().begin();
        it != r.get_peer_partitions().end(); ++it)
    {
        out.append(*it);
    }
    return out;
}

void make_route_state_bindings()
{
    bpl::class_<route_state, boost::noncopyable>("RouteState", bpl::init<>())
        .def("set_key", &py_set_key)
        .def("get_key", &route_state::get_key,
            bpl::return_value_policy<bpl::copy_const_reference>())
        .def("set_ring_position", &route_state::set_ring_position)
        .def("get_ring_position", &route_state::get_ring_position)
        .def("set_primary_partition_uuid",
            &route_state::set_primary_partition_uuid)
        .def("has_primary_partition_uuid",
            &route_state::has_primary_partition_uuid)
        .def("get_primary_partition_uuid",
            &route_state::get_primary_partition_uuid,
            bpl::return_value_policy<bpl::copy_const_reference>())
        .def("get_primary_partition",
            &route_state::get_primary_partition,
            bpl::return_value_policy<bpl::copy_const_reference>())
        .def("add_peer_partition_uuid", &route_state::add_peer_partition_uuid)
        .def("has_peer_partition_uuids",
            &route_state::has_peer_partition_uuids)
        .def("get_peer_partition_uuids", &py_get_peer_partition_uuids)
        .def("get_peer_partitions", &py_get_peer_partitions)
        .def("load_route_state", &route_state::load_route_state)
        .def("reset_route_state", &route_state::reset_route_state)
        ;
}

}
}

