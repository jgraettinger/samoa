
#include "samoa/server/peer_set.hpp"
#include "samoa/server/peer_discovery.hpp"
#include "samoa/server/partition.hpp"
#include "samoa/server/client.hpp"
#include "samoa/server/context.hpp"
#include "samoa/request/request_state.hpp"
#include "samoa/error.hpp"
#include "samoa/log.hpp"
#include <boost/unordered_set.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <functional>

namespace samoa {
namespace server {

bool peer_set::_discovery_enabled = true;

peer_set::peer_set(const spb::ClusterState & state, const ptr_t & current)
{
    auto it = state.peer().begin();
    auto last_it = it;

    for(; it != state.peer().end(); ++it)
    {
        SAMOA_ASSERT(it == last_it || last_it->uuid() < it->uuid());
        last_it = it;

        core::uuid peer_uuid = core::parse_uuid(it->uuid());

        set_server_address(peer_uuid, it->hostname(), it->port());

        if(current && current->has_server(peer_uuid))
        {
            set_connected_server(peer_uuid, current->get_server(peer_uuid));

            _discovery_functors[peer_uuid] = \
                current->_discovery_functors[peer_uuid];
        }
        else
            _discovery_functors[peer_uuid] = peer_discovery::ptr_t();
    }

    // also set the loop-back server address
    core::uuid local_uuid = core::parse_uuid(state.local_uuid());
    set_server_address(local_uuid, "localhost", state.local_port());

    if(current && current->has_server(local_uuid))
    {
        set_connected_server(local_uuid, current->get_server(local_uuid));
    }
}

void peer_set::initialize(const context::ptr_t & context)
{
    server_pool::connect();

    for(discovery_functors_t::value_type & entry : _discovery_functors)
    {
        if(entry.second)
            continue;

        entry.second = boost::make_shared<peer_discovery>(
            context, entry.first);

        if(!_discovery_enabled)
        	continue;

        // initiate discovery for new peer
        (*entry.second)();
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
                core::parse_uuid(p_it->server_uuid()));
        }
    }

    required_peers.erase(core::parse_uuid(local.local_uuid()));

    auto p_it = peer.peer().begin();
    auto l_it = local.peer().begin();

    // closure to remove a local peer at the current iterator position
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

    // closure to insert a local peer at the current iterator position
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

    // linear merge of the local & remote peer lists
    while(p_it != peer.peer().end())
    {
        // check peer-record order invariant
        SAMOA_ASSERT(last_p_it == p_it || last_p_it->uuid() < p_it->uuid());
        last_p_it = p_it;

        if(l_it == local.peer().end() || p_it->uuid() < l_it->uuid())
        {
            // we don't know of this peer-record
            if(required_peers.find(core::parse_uuid(
                p_it->uuid())) != required_peers.end())
            {
                LOG_INFO("discovered indirect peer " << \
                    log::ascii_escape(p_it->uuid()));

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
            if(required_peers.find(core::parse_uuid(
                l_it->uuid())) == required_peers.end())
            {
                LOG_INFO("dropped peer " << log::ascii_escape(l_it->uuid()));
                remove_record();
            }
            else
                ++l_it;
        }
        else
        {
            // l_it & p_it reference the same peer-record
            if(!l_it->seed() && 
                required_peers.find(core::parse_uuid(
                    l_it->uuid())) == required_peers.end())
            {
                LOG_INFO("dropped peer " << log::ascii_escape(l_it->uuid()));
                remove_record();
            }
            else
                ++l_it;

            ++p_it;
        }
    }
    while(l_it != local.peer().end())
    {
        if(required_peers.find(core::parse_uuid(
            l_it->uuid())) == required_peers.end())
        {
            LOG_INFO("dropped peer " << log::ascii_escape(l_it->uuid()));
            remove_record();
        }
        else
            ++l_it;
    }

    // should we have a record for the peer itself?
    if(required_peers.find(core::parse_uuid(
        peer.local_uuid())) != required_peers.end())
    {
        // identify where the record should be located
        l_it = std::lower_bound(local.peer().begin(), local.peer().end(),
            peer.local_uuid(),
            [](const spb::ClusterState::Peer & peer,
               const std::string & peer_uuid) -> bool
            {
                return peer.uuid() < peer_uuid;
            });

        // if a record isn't present, add one
        if(l_it == local.peer().end() || l_it->uuid() != peer.local_uuid())
        {
            spb::ClusterState::Peer * new_peer = add_record();

            new_peer->set_uuid(peer.local_uuid());
            new_peer->set_hostname(peer.local_hostname());
            new_peer->set_port(peer.local_port());

            LOG_INFO("discovered direct peer " << \
                log::ascii_escape(peer.local_uuid()));
        }
    }
}

core::uuid peer_set::select_best_peer(const request::state::ptr_t & rstate)
{
    SAMOA_ASSERT(rstate->has_peer_partition_uuids());

    unsigned best_peer_latency = std::numeric_limits<unsigned>::max();
    core::uuid best_peer_uuid = boost::uuids::nil_uuid();

    for(const partition::ptr_t & partition : rstate->get_peer_partitions())
    {
        unsigned peer_latency = std::numeric_limits<unsigned>::max();
        core::uuid peer_uuid = partition->get_server_uuid();

        // TODO(johng): factor actual latency into this selection
        samoa::client::server::ptr_t server = get_server(peer_uuid);
        if(server)
        {
            peer_latency = 0;
        }

        if(peer_latency < best_peer_latency || best_peer_uuid.is_nil())
        {
            best_peer_uuid = peer_uuid;
        }
    }
    return best_peer_uuid;
}

void peer_set::forward_request(request::state::ptr_t rstate)
{
    SAMOA_ASSERT(!rstate->get_primary_partition());

    if(rstate->get_peer_partitions().empty())
    {
        rstate->send_error(404, "no partitions for forwarding");
        return;
    }

    core::uuid best_peer_uuid = select_best_peer(rstate);

    schedule_request(
        std::bind(&peer_set::on_forwarded_request,
            std::placeholders::_1,
            std::placeholders::_2,
            std::move(rstate),
            best_peer_uuid),
        best_peer_uuid);
}

void peer_set::begin_peer_discovery()
{
	if(!_discovery_enabled)
        return;

    for(const discovery_functors_t::value_type & entry : _discovery_functors)
    {
        (*entry.second)();
    }
}

void peer_set::begin_peer_discovery(const core::uuid & peer_uuid)
{
    discovery_functors_t::const_iterator it = \
        _discovery_functors.find(peer_uuid);

    SAMOA_ASSERT(it != _discovery_functors.end());

	if(!_discovery_enabled)
        return;

    (*it->second)();
}

/* static */
void peer_set::on_forwarded_request(
    boost::system::error_code ec,
    samoa::client::server_request_interface iface,
    const request::state::ptr_t & rstate,
    core::uuid peer_uuid)
{
    if(ec)
    {
        rstate->send_error(500, ec);
        return;
    }

    iface.get_message().CopyFrom(rstate->get_samoa_request());
    iface.get_message().clear_data_block_length();

    for(const auto & block : rstate->get_request_data_blocks())
    {
        iface.add_data_block(block);
    }

    iface.flush_request(
        std::bind(&peer_set::on_forwarded_response,
            std::placeholders::_1,
            std::placeholders::_2,
            rstate, peer_uuid));
}

/* static */
void peer_set::on_forwarded_response(
    boost::system::error_code ec,
    samoa::client::server_response_interface iface,
    const request::state::ptr_t & rstate,
    core::uuid peer_uuid)
{
    if(ec)
    {
        rstate->send_error(500, ec);
        return;
    }

    rstate->get_samoa_response().CopyFrom(iface.get_message());
    rstate->get_samoa_response().clear_data_block_length();

    for(const auto & block : iface.get_response_data_blocks())
    {
        rstate->add_response_data_block(block);
    }
    rstate->flush_response();

    if(iface.get_error_code() == 404)
    {
        // cluster-state may be inconsistent
        rstate->get_context()->get_cluster_state()->get_peer_set(
            )->begin_peer_discovery(peer_uuid);
    }
    iface.finish_response();
}

}
}

