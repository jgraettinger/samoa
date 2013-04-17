#include "pysamoa/boost_python.hpp"
#include "samoa/server/peer_set.hpp"

namespace samoa {
namespace server {

namespace bpl = boost::python;

void make_peer_set_bindings()
{
    void (peer_set::*bpd_ptr1)() = &peer_set::begin_peer_discovery;
    void (peer_set::*bpd_ptr2)(const core::uuid &) = \
        &peer_set::begin_peer_discovery;

    bpl::class_<peer_set, peer_set::ptr_t,
        bpl::bases<samoa::client::server_pool>,
        boost::noncopyable>(
            "PeerSet", bpl::init<
                const spb::ClusterState &,
                const peer_set::ptr_t &>())
        .def("merge_peer_set", &peer_set::merge_peer_set)
        .def("forward_request", &peer_set::forward_request)
        .def("begin_peer_discovery", bpd_ptr1)
        .def("begin_peer_discovery", bpd_ptr2)
        .def("set_discovery_enabled", &peer_set::set_discovery_enabled)
        .staticmethod("set_discovery_enabled")
        ;
}

}
}

