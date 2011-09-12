
#include <boost/python.hpp>
#include "samoa/server/peer_set.hpp"

namespace samoa {
namespace server {

namespace bpl = boost::python;

void make_peer_set_bindings()
{
    bpl::class_<peer_set, peer_set::ptr_t,
        bpl::bases<samoa::client::server_pool>,
        boost::noncopyable>(
            "PeerSet", bpl::init<
                const spb::ClusterState &,
                const peer_set::ptr_t &>())
        .def("merge_peer_set", &peer_set::merge_peer_set)
        .def("forward_request", &peer_set::forward_request)
        ;
}

}
}

