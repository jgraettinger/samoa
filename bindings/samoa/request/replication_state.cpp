
#include <boost/python.hpp>
#include "samoa/request/replication_state.hpp"

namespace samoa {
namespace request {

namespace bpl = boost::python;

void make_replication_state_bindings()
{
    bpl::class_<replication_state, boost::noncopyable>(
            "ReplicationState", bpl::init<>())
        .def("set_quorum_count", &replication_state::set_quorum_count)
        .def("get_quorum_count", &replication_state::get_quorum_count)
        .def("get_peer_success_count",
            &replication_state::get_peer_success_count)
        .def("get_peer_failure_count",
            &replication_state::get_peer_failure_count)
        .def("peer_replication_failure",
            &replication_state::peer_replication_failure)
        .def("peer_replication_success",
            &replication_state::peer_replication_success)
        .def("is_replication_finished",
            &replication_state::is_replication_finished)
        .def("had_peer_read_hit",
            &replication_state::had_peer_read_hit)
        .def("set_peer_read_hit",
            &replication_state::set_peer_read_hit)
        .def("get_replication_checksum",
            &replication_state::get_replication_checksum,
            bpl::return_value_policy<copy_const_reference>())
        .def("set_replication_checksum",
            &replication_state::set_replication_checksum)
        .def("load_replication_state",
            &replication_state::load_replication_state)
        .def("reset_replication_state",
            &replication_state::reset_replication_state)
        ;
}

}
}

