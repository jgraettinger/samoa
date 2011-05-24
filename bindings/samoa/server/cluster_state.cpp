
#include <boost/python.hpp>
#include "samoa/server/cluster_state.hpp"

namespace samoa {
namespace server {

namespace bpl = boost::python;

void make_cluster_state_bindings()
{
    bpl::class_<cluster_state, cluster_state::ptr_t, boost::noncopyable>(
        "ClusterState", bpl::no_init)
        .def("get_protobuf_description", &cluster_state::get_protobuf_description,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("get_peer_set", &cluster_state::get_peer_set,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("get_table_set", &cluster_state::get_table_set,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("merge_cluster_state", &cluster_state::merge_cluster_state);
}

}
}

