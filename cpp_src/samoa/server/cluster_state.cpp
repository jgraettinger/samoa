
#include "samoa/server/cluster_state.hpp"
#include "samoa/server/peer_set.hpp"
#include "samoa/server/table_set.hpp"

namespace samoa {
namespace server {

cluster_state::cluster_state(
    std::unique_ptr<spb::ClusterState> && desc,
    const ptr_t & current)
 :  _desc(std::move(desc))
{
    _peer_set = boost::make_shared<peer_set>(
        *_desc, current ? current->_peer_set : peer_set::ptr_t());
    _table_set = boost::make_shared<table_set>(
        *_desc, current ? current->_table_set : table_set::ptr_t());
}

bool cluster_state::merge_cluster_state(
    const spb::ClusterState & peer_state,
    spb::ClusterState & local_state) const
{
    if(!_table_set->merge_table_set(peer_state, local_state))
    {
        // if table-set didn't change, peer-set won't either
        return false;
    }

    _peer_set->merge_peer_set(peer_state, local_state);
    return true;
}

}
}

