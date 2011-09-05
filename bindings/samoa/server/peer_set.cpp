
#include <boost/python.hpp>
#include "samoa/server/peer_set.hpp"

namespace samoa {
namespace server {

namespace bpl = boost::python;

void make_peer_set_bindings()
{
    void (peer_set::*forward_request_uuid)(
        const client_ptr_t &, const core::uuid &) = \
        &peer_set::forward_request;

    void (peer_set::*forward_request_server)(
        const client_ptr_t &, const samoa::client::server_ptr_t &) = \
        &peer_set::forward_request;

    bpl::class_<peer_set, peer_set::ptr_t,
        bpl::bases<samoa::client::server_pool>,
        boost::noncopyable>(
            "PeerSet", bpl::init<
                const spb::ClusterState &,
                const peer_set::ptr_t &>())
        .def("merge_peer_set", &peer_set::merge_peer_set)
        .def("forward_request", forward_request_uuid)
        .def("forward_request", forward_request_server)
        ;
}

}
}

