
#include <boost/python.hpp>
#include "samoa/server/partition_peer.hpp"
#include "samoa/client/server.hpp"
#include "samoa/server/partition.hpp"

namespace samoa {
namespace server {

namespace bpl = boost::python;

void make_partition_peer_bindings()
{
    bpl::class_<partition_peer>("PartitionPeer", bpl::init<>())
        .def(bpl::init<const partition_ptr_t &,
            const samoa::client::server_ptr_t &>())
        .def_readwrite("partition", &partition_peer::partition)
        .def_readwrite("server", &partition_peer::server)
        ;
}

}
}

