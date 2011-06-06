
#include "samoa/server/peer_set.hpp"
#include "samoa/error.hpp"
#include "samoa/log.hpp"
#include <boost/unordered_set.hpp>

namespace samoa {
namespace server {

peer_set::peer_set(const spb::ClusterState & state, const ptr_t & current)
{
    auto it = state.peer().begin();
    auto last_it = it;

    for(; it != state.peer().end(); ++it)
    {
        SAMOA_ASSERT(it == last_it || last_it->uuid() < it->uuid());
        last_it = it;

        core::uuid uuid = core::uuid_from_hex(it->uuid());

        set_server_address(core::uuid_from_hex(it->uuid()),
            it->hostname(), it->port());

        if(current && current->has_server(uuid))
        {
            set_connected_server(uuid, current->get_server(uuid));
        }
    }
}

void peer_set::merge_peer_set(const spb::ClusterState & peer,
    spb::ClusterState & local) const
{
    // collect uuids of all servers we must peer with
    boost::unordered_set<core::uuid> required_peers;

    // iterate through remote partitions
    for(auto t_it = local.table().begin();
        t_it != local.table().end(); ++t_it)
    {
        for(auto p_it = t_it->partition().begin();
            p_it != t_it->partition().end(); ++p_it)
        {
            if(p_it->dropped())
                continue;

            required_peers.insert(
                core::uuid_from_hex(p_it->server_uuid()));
        }
    }

    required_peers.erase(
        core::uuid_from_hex(local.local_uuid()));

    auto p_it = peer.peer().begin();
    auto l_it = local.peer().begin();

    auto remove_record = [&]() -> void
    {
        int ind = std::distance(local.peer().begin(), l_it);

        // bubble element to list tail
        for(int cur_ind = ind + 1; cur_ind != local.peer().size(); ++cur_ind)
        {
            local.mutable_peer()->SwapElements(cur_ind - 1, cur_ind);
        }
        local.mutable_peer()->RemoveLast();

        // restore iterator position
        l_it = local.peer().begin() + ind; 
    };

    auto add_record = [&]() -> spb::ClusterState::Peer *
    {
        int ind = std::distance(local.peer().begin(), l_it);
        spb::ClusterState::Peer * rec = local.mutable_peer()->Add();

        // bubble new element into place
        for(int cur_ind = local.peer().size(); --cur_ind != ind;)
        {
            local.mutable_peer()->SwapElements(cur_ind, cur_ind - 1);
        }

        // restore iterator
        l_it = local.peer().begin() + ind;
        return rec;
    };

    auto last_p_it = p_it;

    while(p_it != peer.peer().end())
    {
        // check peer-record order invariant
        SAMOA_ASSERT(last_p_it == p_it || last_p_it->uuid() < p_it->uuid());
        last_p_it = p_it;

        if(l_it == local.peer().end() || p_it->uuid() < l_it->uuid())
        {
            // we don't know of this peer-record
            if(required_peers.find(core::uuid_from_hex(
                p_it->uuid())) != required_peers.end())
            {
                LOG_INFO("discovered peer " << p_it->uuid());

                spb::ClusterState::Peer * new_peer = add_record();

                new_peer->set_uuid(p_it->uuid());
                new_peer->set_hostname(p_it->hostname());
                new_peer->set_port(p_it->port());

                ++l_it; 
            }
            ++p_it;
        }
        else if(l_it->uuid() < p_it->uuid())
        {
            // peer doesn't know of this peer-record
            if(required_peers.find(core::uuid_from_hex(
                l_it->uuid())) == required_peers.end())
            {
                LOG_INFO("dropped peer " << l_it->uuid());
                remove_record();
            }
            else
                ++l_it;
        }
        else
        {
            // l_it & p_it reference the same peer-record
            if(required_peers.find(core::uuid_from_hex(
                l_it->uuid())) == required_peers.end())
            {
                LOG_INFO("dropped peer " << l_it->uuid());
                remove_record();
            }
            else
                ++l_it;

            ++p_it;
        }
    }
    while(l_it != local.peer().end())
    {
        if(required_peers.find(core::uuid_from_hex(
            l_it->uuid())) == required_peers.end())
        {
            LOG_INFO("dropped peer " << l_it->uuid());
            remove_record();
        }
        else
            ++l_it;
    }
}

}
}

