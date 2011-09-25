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
#include <boost/bind.hpp>

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
    _error_count(0),
    _success_count(0),
    _flush_response_called(false),
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

    if(_samoa_request.requested_quorum() > 0)
    {
        _client_quorum = _samoa_request.requested_quorum() - 1;
    }
    else
    {
        // 0 is interpreted as "All peers"
        _client_quorum = _partition_peers.size();
    }

    if(_client_quorum > _partition_peers.size())
    {
        err << "quorum count " << _client_quorum;
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

    return true;
}

bool request_state::peer_replication_failure()
{
    ++_error_count;

    // did we already succeed?
    if(_success_count == _client_quorum)
    {
        return false;
    }

    // is this the last outstanding replication?
    return _error_count + _success_count == _partition_peers.size();
}

bool request_state::peer_replication_success()
{
    if(++_success_count == _client_quorum)
    {
        return true;
    }

    // is this the last outstanding replication?
    return _error_count + _success_count == _partition_peers.size();
}

bool request_state::is_client_quorum_met() const
{
    return _success_count >= _client_quorum ||
        _error_count + _success_count == _partition_peers.size();
}

void request_state::add_response_data_block(
    const core::const_buffer_regions_t & bs)
{
    SAMOA_ASSERT(!_flush_response_called);

    size_t length = 0;

    std::for_each(bs.begin(), bs.end(),
        [&length](const core::const_buffer_region & b)
        { length += b.size(); });

    _samoa_response.add_data_block_length(length);
    _response_data.insert(_response_data.end(), bs.begin(), bs.end());
}

void request_state::add_response_data_block(const core::buffer_regions_t & bs)
{ 
    SAMOA_ASSERT(!_flush_response_called);

    size_t length = 0;

    std::for_each(bs.begin(), bs.end(),
        [&length](const core::buffer_region & b)
        { length += b.size(); });

    _samoa_response.add_data_block_length(length);
    _response_data.insert(_response_data.end(), bs.begin(), bs.end());
}

void request_state::flush_client_response()
{
    SAMOA_ASSERT(!_flush_response_called);
    _flush_response_called = true;

    _client->schedule_response(
        boost::bind(&request_state::on_client_response,
            shared_from_this(), _1));
}

void request_state::send_client_error(unsigned err_code,
    const std::string & err_msg)
{
    _samoa_response.Clear();
    _response_data.clear();

    _samoa_response.set_type(core::protobuf::ERROR);
    _samoa_response.mutable_error()->set_code(err_code);
    _samoa_response.mutable_error()->set_message(err_msg);

    flush_client_response();
}

void request_state::send_client_error(unsigned err_code,
    const boost::system::error_code & ec)
{
    std::stringstream tmp;
    tmp << ec << " (" << ec.message() << ")";

    send_client_error(err_code, tmp.str());
}

void request_state::on_client_response(client::response_interface iface)
{
    // serlialize & queue core::protobuf::SamoaResponse for writing
    core::zero_copy_output_adapter zco_adapter;
    _samoa_response.SerializeToZeroCopyStream(&zco_adapter);

    SAMOA_ASSERT(zco_adapter.ByteCount() < (1<<16));

    // write network order unsigned short length
    uint16_t len = htons((uint16_t)zco_adapter.ByteCount());
    iface.write_interface().queue_write((char*)&len, ((char*)&len) + 2);

    // write serialized SamoaResponse output regions
    iface.write_interface().queue_write(zco_adapter.output_regions());

    // write spooled data blocks
    iface.write_interface().queue_write(_response_data);

    // flush writes; release ownership of response_interface
    iface.finish_response();
}



}
}

