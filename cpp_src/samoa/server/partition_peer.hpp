#ifndef SAMOA_SERVER_PARTITION_PEER_HPP
#define SAMOA_SERVER_PARTITION_PEER_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/client/fwd.hpp"
#include <list>

namespace samoa {
namespace server {

struct partition_peer
{
    partition_peer()
    { }

    partition_peer(
        const partition_ptr_t & partition,
        const samoa::client::server_ptr_t & server)
     :  partition(partition),
        server(server)
    { }

    partition_ptr_t partition;
    samoa::client::server_ptr_t server;
};

typedef std::list<partition_peer> partition_peers_t;

}
}

#endif

