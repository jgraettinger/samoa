
#include "samoa/request/client_state.hpp"
#include "samoa/request/request_state.hpp"
#include "samoa/request/state_exception.hpp"
#include "samoa/datamodel/clock_util.hpp"
#include "samoa/server/client.hpp"
#include "samoa/core/uuid.hpp"
#include <boost/bind.hpp>

namespace samoa {
namespace request {

client_state::client_state()
 : _flush_response_called(false)
{ }

client_state::~client_state()
{ }

void client_state::validate_samoa_request_syntax()
{
    const spb::SamoaRequest & request = get_samoa_request();

    if(request.has_table_uuid() &&
       core::try_parse_uuid(request.table_uuid()).is_nil())
    {
        std::stringstream err;
        err << "malformed table-uuid " << request.table_uuid();
        throw state_exception(400, err.str());
    }

    if(request.has_partition_uuid() &&
        core::try_parse_uuid(request.partition_uuid()).is_nil())
    {
        std::stringstream err;
        err << "malformed partition-uuid " << request.partition_uuid();
        throw state_exception(400, err.str());
    }

    for(auto it = request.peer_partition_uuid().begin();
        it != request.peer_partition_uuid().end(); ++it)
    {
        if(core::try_parse_uuid(*it).is_nil())
        {
            std::stringstream err;
            err << "malformed peer-partition-uuid " << *it;
            throw state_exception(400, err.str());
        }
    }

    if(request.has_cluster_clock() &&
       !datamodel::clock_util::validate(request.cluster_clock()))
    {
        std::stringstream err;
        err << "malformed cluster-clock";
        throw state_exception(400, err.str());
    }
}

void client_state::add_response_data_block(
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

void client_state::add_response_data_block(const core::buffer_regions_t & bs)
{ 
    SAMOA_ASSERT(!_flush_response_called);

    size_t length = 0;

    std::for_each(bs.begin(), bs.end(),
        [&length](const core::buffer_region & b)
        { length += b.size(); });

    _samoa_response.add_data_block_length(length);
    _response_data.insert(_response_data.end(), bs.begin(), bs.end());
}

void client_state::flush_response(const state::ptr_t & guard)
{
    SAMOA_ASSERT(!_flush_response_called);
    _flush_response_called = true;

    _client->schedule_response(
        // pass this as argument to binder, but also pass a new
        //  reference to guard the lifetime of the request_state
        boost::bind(&client_state::on_response, this, _1, guard));
}

void client_state::send_error(const state::ptr_t & guard,
    unsigned err_code, const std::string & err_msg)
{
    _samoa_response.Clear();
    _response_data.clear();

    _samoa_response.set_type(core::protobuf::ERROR);
    _samoa_response.mutable_error()->set_code(err_code);
    _samoa_response.mutable_error()->set_message(err_msg);

    flush_response(guard);
}

void client_state::send_error(const state::ptr_t & guard,
    unsigned err_code, const boost::system::error_code & ec)
{
    std::stringstream tmp;
    tmp << ec << " (" << ec.message() << ")";

    send_error(guard, err_code, tmp.str());
}

void client_state::load_client_state(const server::client::ptr_t & client)
{
    SAMOA_ASSERT(!_client);
    _client = client;
}

void client_state::reset_client_state()
{
    _client.reset();
    _samoa_request.Clear();
    _samoa_response.Clear();
    _request_data_blocks.clear();
    _response_data.clear();
    _flush_response_called = false;
}

void client_state::on_response(server::client::response_interface iface,
    const state::ptr_t & guard)
{
    // set the response request_id to that of the request
    _samoa_response.set_request_id(_samoa_request.request_id());

    // if the response has no type, set it to that of the request
    if(!_samoa_response.has_type())
    {
        _samoa_response.set_type(_samoa_request.type());
    }

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

