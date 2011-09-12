#include "samoa/server/request_state.hpp"
#include "samoa/server/client.hpp"
#include "samoa/server/context.hpp"
#include "samoa/server/cluster_state.hpp"
#include "samoa/server/peer_set.hpp"
#include "samoa/server/local_partition.hpp"
#include "samoa/server/partition.hpp"
#include "samoa/server/table_set.hpp"
#include "samoa/server/table.hpp"
#include "samoa/core/proactor.hpp"

namespace samoa {
namespace server {

request_state::ptr_t request_state::extract(const client::ptr_t & client)
{
    request_state::ptr_t rstate = boost::make_shared<request_state>();

    std::stringstream err;

    const spb::SamoaRequest & samoa_request = client->get_request();
    cluster_state::ptr_t cstate = client->get_context()->get_cluster_state();

    rstate->_client = client;
    rstate->_peer_set = cstate->get_peer_set();

    ///////////// TABLE

    if(samoa_request.has_table_uuid())
    {
        if(samoa_request.table_uuid().size() != sizeof(core::uuid))
        {
            client->send_error(400, "malformed table_uuid");
            return ptr_t();
        }

        core::uuid table_uuid;
        std::copy(samoa_request.table_uuid().begin(),
            samoa_request.table_uuid().end(),
            table_uuid.begin());

        rstate->_table = cstate->get_table_set()->get_table(table_uuid);

        if(!rstate->_table)
        {
            err << "table_uuid " << table_uuid;
            client->send_error(404, err.str());
            return ptr_t();
        }
    }
    else if(samoa_request.has_table_name())
    {
        rstate->_table = cstate->get_table_set()->get_table_by_name(
            samoa_request.table_name());

        if(!rstate->_table)
        {
            err << "table_name " << samoa_request.table_name();
            client->send_error(404, err.str());
            return ptr_t();
        }
    }
    // table is required by key & partition_uuid
    else if(samoa_request.has_key() ||
            samoa_request.has_partition_uuid())
    {
        client->send_error(400, "expected table_uuid or table_name");
        return ptr_t();
    }

    ///////////// KEY

    if(samoa_request.has_key())
    {
        rstate->_key = samoa_request.key();
    }
    // key is required by partition_uuid
    else if(samoa_request.has_partition_uuid())
    {
        client->send_error(400, "expected key");
        return ptr_t();
    }

    ///////////// PARTITION_UUID / PEER_PARTITION_UUID

    if(samoa_request.has_partition_uuid())
    {
        core::uuid partition_uuid;
        std::copy(samoa_request.partition_uuid().begin(),
            samoa_request.partition_uuid().end(),
            partition_uuid.begin());

        rstate->_primary_partition = \
            boost::dynamic_pointer_cast<local_partition>(
                rstate->_table->get_partition(partition_uuid));

        if(!rstate->_primary_partition)
        {
            err << "partition_uuid " << partition_uuid;
            client->send_error(404, err.str());
            return ptr_t();
        }

        uint64_t ring_pos = rstate->_table->ring_position(rstate->_key);

        if(!rstate->_primary_partition->position_in_responsible_range(ring_pos))
        {
            err << "key " << rstate->_key << " maps to position " << ring_pos;
            err << " but partition_uuid " << partition_uuid << " has range (";
            err << rstate->_primary_partition->get_range_begin() << ", ";
            err << rstate->_primary_partition->get_range_end() << ")";

            client->send_error(409, err.str());
            return ptr_t();
        }

        for(auto it = samoa_request.peer_partition_uuid().begin();
            it != samoa_request.peer_partition_uuid().end(); ++it)
        {
            core::uuid peer_part_uuid;
            std::copy(it->begin(), it->end(), peer_part_uuid.begin());

            partition_peer peer;
            peer.partition = rstate->_table->get_partition(peer_part_uuid);

            if(!peer.partition)
            {
                err << "peer_partition_uuid " << peer_part_uuid;
                client->send_error(404, err.str());
                return ptr_t();
            }
            if(!peer.partition->position_in_responsible_range(ring_pos))
            {
                err << "key " << rstate->_key << " maps to position ";
                err << ring_pos << " but peer_partition_uuid ";
                err << peer_part_uuid << " has range (";
                err << peer.partition->get_range_begin() << ", ";
                err << peer.partition->get_range_end() << ")";

                client->send_error(409, err.str());
                return ptr_t();
            }

            peer.server = rstate->_peer_set->get_server(
                peer.partition->get_server_uuid());
        }
    }
    // partition_uuid is required by peer_partition_uuid
    else if(samoa_request.peer_partition_uuid_size())
    {
        client->send_error(400, "expected partition_uuid");
        return ptr_t();
    }
    else if(samoa_request.has_key())
    {
        // route key to primary partition / peer partitions
        uint64_t ring_pos = rstate->_table->ring_position(rstate->_key);

        rstate->_table->route_ring_position(
            ring_pos,
            rstate->_peer_set,
            rstate->_primary_partition,
            rstate->_partition_peers);
    }

    ///////////// QUORUM

    rstate->_quorum_count = samoa_request.quorum();
    rstate->_error_count = 0;
    rstate->_success_count = 0;

    if(rstate->_quorum_count > rstate->_partition_peers.size() + 1)
    {
        err << "quorum count " << rstate->_quorum_count << " greater than ";
        err << "effective replication factor ";
        err << (1 + rstate->_partition_peers.size());
        client->send_error(400, err.str());
    }

    rstate->_io_srv = core::proactor::get_proactor()->serial_io_service();

    return rstate;
}

bool request_state::replication_failure()
{ return ++_error_count + _quorum_count == _partition_peers.size() + 1; }

bool request_state::replication_success()
{ return ++_success_count == _quorum_count; }

bool request_state::replication_complete()
{
    return _success_count >= _quorum_count ||
        _error_count + _success_count >= _partition_peers.size();
}

void request_state::send_client_error(unsigned code,
    const std::string & msg)
{
    _client->send_error(code, msg);
    _client.reset();
}

void request_state::send_client_error(unsigned code,
    const boost::system::error_code & ec)
{
    _client->send_error(code, ec);
    _client.reset();
}

void request_state::finish_client_response()
{
    _client->finish_response();
    _client.reset();
}

}
}

