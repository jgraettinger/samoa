#include "samoa/server/request_state.hpp"
#include "samoa/server/client.hpp"
#include "samoa/server/context.hpp"
#include "samoa/server/cluster_state.hpp"
#include "samoa/server/peer_set.hpp"
#include "samoa/server/local_partition.hpp"
#include "samoa/server/partition.hpp"
#include "samoa/server/table_set.hpp"
#include "samoa/server/table.hpp"
#include "samoa/datamodel/clock_util.hpp"
#include "samoa/core/proactor.hpp"
#include "samoa/log.hpp"
#include <boost/lexical_cast.hpp>

namespace samoa {
namespace server {

bool extraction_error(request_state & rstate,
    unsigned code, const std::string & err)
{
    if(!rstate.get_client())
    {
        // support for unit-tests
        std::stringstream msg;
        msg << "<code " << code << ">: " << err;
        throw std::runtime_error(msg.str());
    }

    rstate.send_client_error(code, err);
    return false;
}

request_state::request_state(const client::ptr_t & client)
 :  _client(client),
    _start_response_called(false),
    _io_srv(client ? client->get_io_service() : \
        core::proactor::get_proactor()->serial_io_service())
{ }

bool request_state::load_from_samoa_request(const context::ptr_t & context)
{
    std::stringstream err;

    _context = context;
    cluster_state::ptr_t cstate = _context->get_cluster_state();
    _peer_set = cstate->get_peer_set();

    ///////////// TABLE

    if(_samoa_request.has_table_uuid())
    {
        if(_samoa_request.table_uuid().size() != sizeof(core::uuid))
        {
            return extraction_error(*this, 400, "malformed table_uuid");
        }

        core::uuid table_uuid;
        std::copy(_samoa_request.table_uuid().begin(),
            _samoa_request.table_uuid().end(),
            table_uuid.begin());

        _table = cstate->get_table_set()->get_table(table_uuid);

        if(!_table)
        {
            err << "table_uuid " << table_uuid;
            return extraction_error(*this, 404, err.str());
        }
    }
    else if(_samoa_request.has_table_name())
    {
        _table = cstate->get_table_set()->get_table_by_name(
            _samoa_request.table_name());

        if(!_table)
        {
            err << "table_name " << _samoa_request.table_name();
            return extraction_error(*this, 404, err.str());
        }
    }
    // table is required by key & partition_uuid
    else if(_samoa_request.has_key() ||
            _samoa_request.has_partition_uuid())
    {
        return extraction_error(*this, 400,
            "expected table_uuid or table_name");
    }

    ///////////// KEY

    if(_samoa_request.has_key())
    {
        _key = _samoa_request.key();
    }
    // key is required by peer_partition_uuid
    else if(_samoa_request.peer_partition_uuid_size())
    {
        return extraction_error(*this, 400, "expected key");
    }

    ///////////// PARTITION_UUID / PEER_PARTITION_UUID

    if(_samoa_request.has_partition_uuid())
    {
        if(_samoa_request.partition_uuid().size() != sizeof(core::uuid))
        {
            return extraction_error(*this, 400, "malformed partition_uuid");
        }

        core::uuid partition_uuid;
        std::copy(_samoa_request.partition_uuid().begin(),
            _samoa_request.partition_uuid().end(),
            partition_uuid.begin());

        _primary_partition = \
            boost::dynamic_pointer_cast<local_partition>(
                _table->get_partition(partition_uuid));

        if(!_primary_partition)
        {
            err << "partition_uuid " << partition_uuid;
            return extraction_error(*this, 404, err.str());
        }

        uint64_t ring_pos = _table->ring_position(_key);

        if(!_key.empty() &&
           !_primary_partition->position_in_responsible_range(ring_pos))
        {
            err << "key " << _key << " maps to position " << ring_pos;
            err << " but partition_uuid " << partition_uuid << " has range (";
            err << _primary_partition->get_range_begin() << ", ";
            err << _primary_partition->get_range_end() << ")";

            return extraction_error(*this, 409, err.str());
        }

        for(auto it = _samoa_request.peer_partition_uuid().begin();
            it != _samoa_request.peer_partition_uuid().end(); ++it)
        {
            if(it->size() != sizeof(core::uuid))
            {
                return extraction_error(*this, 400,
                    "malformed peer_partition_uuid");
            }

            core::uuid peer_part_uuid;
            std::copy(it->begin(), it->end(), peer_part_uuid.begin());

            _partition_peers.push_back(partition_peer());
            partition_peer & peer = _partition_peers.back();

            peer.partition = _table->get_partition(peer_part_uuid);

            if(!peer.partition)
            {
                err << "peer_partition_uuid " << peer_part_uuid;
                return extraction_error(*this, 404, err.str());
            }
            if(!peer.partition->position_in_responsible_range(ring_pos))
            {
                err << "key " << _key << " maps to position ";
                err << ring_pos << " but peer_partition_uuid ";
                err << peer_part_uuid << " has range (";
                err << peer.partition->get_range_begin() << ", ";
                err << peer.partition->get_range_end() << ")";

                return extraction_error(*this, 409, err.str());
            }

            peer.server = _peer_set->get_server(
                peer.partition->get_server_uuid());
        }
    }
    // partition_uuid is required by peer_partition_uuid
    else if(_samoa_request.peer_partition_uuid_size())
    {
        return extraction_error(*this, 400, err.str());
    }
    else if(_samoa_request.has_key())
    {
        // route key to primary partition / peer partitions
        uint64_t ring_pos = _table->ring_position(_key);

        _table->route_ring_position(
            ring_pos, _peer_set, _primary_partition, _partition_peers);

        if(!_primary_partition && _partition_peers.empty())
        {
            return extraction_error(*this, 410, "no routable partition");
        }
    }

    ///////////// QUORUM

    _quorum_count = _samoa_request.quorum();
    _error_count = 0;
    _success_count = 0;

    if(_quorum_count > _partition_peers.size() + 1)
    {
        err << "quorum count " << _quorum_count;
        err << " greater than effective replication factor ";
        err << (1 + _partition_peers.size());
        return extraction_error(*this, 400, err.str());
    }

    ///////////// CLUSTER CLOCK

    if(_samoa_request.has_cluster_clock() &&
        !datamodel::clock_util::validate(_samoa_request.cluster_clock()))
    {
        return extraction_error(*this, 400, "malformed cluster clock");
    }

    /////////////

    // preset the response type to that of the request
    _samoa_response.set_type(_samoa_request.type());

    // also set 'closing' if the client requested it
    if(_samoa_request.closing())
        _samoa_response.set_closing(true);

    return true;
}

bool request_state::peer_replication_failure()
{ return _error_count++ == (1 + _partition_peers.size() - _quorum_count); }

bool request_state::peer_replication_success()
{ return ++_success_count + 1 == _quorum_count; }

bool request_state::is_replication_complete() const
{
    return _success_count + 1 >= _quorum_count ||
        _error_count > (1 + _partition_peers.size() - _quorum_count);
}

void request_state::send_client_error(unsigned err_code,
    const std::string & err_msg, bool closing /* = false */)
{
    _samoa_response.Clear();

    _samoa_response.set_type(core::protobuf::ERROR);
    _samoa_response.set_closing(closing);
    _samoa_response.mutable_error()->set_code(err_code);
    _samoa_response.mutable_error()->set_message(err_msg);

    finish_client_response();
}

void request_state::send_client_error(unsigned err_code,
    const boost::system::error_code & ec, bool closing /* = false */)
{
    std::stringstream tmp;
    tmp << ec << " (" << ec.message() << ")";

    send_client_error(err_code, tmp.str(), closing);
}

void request_state::start_client_response()
{
    SAMOA_ASSERT(_client);

    if(_start_response_called)
    {
        return;
    }

    SAMOA_ASSERT(!_client->has_queued_writes());

    _start_response_called = true;

    // serlialize & queue core::protobuf::SamoaResponse for writing
    core::zero_copy_output_adapter zco_adapter;
    _samoa_response.SerializeToZeroCopyStream(&zco_adapter);

    SAMOA_ASSERT(zco_adapter.ByteCount() < (1<<16));

    // write network order unsigned short length
    uint16_t len = htons((uint16_t)zco_adapter.ByteCount());
    _client->write_interface().queue_write((char*)&len, ((char*)&len) + 2);

    // write serialized output regions
    _client->write_interface().queue_write(zco_adapter.output_regions());
}

void request_state::finish_client_response()
{
    start_client_response();

    _client->finish_response(_samoa_response.closing());
    _client.reset();
}


}
}

