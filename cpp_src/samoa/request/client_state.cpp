
#include "samoa/request/client_state.hpp"
#include "samoa/request/request_state.hpp"
#include "samoa/request/state_exception.hpp"
#include "samoa/server/client.hpp"
#include "samoa/core/uuid.hpp"
#include "samoa/core/protobuf/zero_copy_output_adapter.hpp"
#include <functional>

namespace samoa {
namespace request {

client_state::client_state()
 : _flush_response_called(false)
{ }

client_state::~client_state()
{ }

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

void client_state::flush_response(state::ptr_t guard)
{
    SAMOA_ASSERT(_client && !_flush_response_called);
    _flush_response_called = true;

    _client->schedule_response(
        // pass 'this' as argument to binder, but also pass 'guard'
        //  by-value, to protect the lifetime of the request state
        std::bind(&client_state::on_schedule_response, this,
            std::placeholders::_1,
            std::placeholders::_2,
            std::move(guard)));
}

void client_state::send_error(state::ptr_t guard,
    unsigned err_code, const std::string & err_msg)
{
    _samoa_response.Clear();
    _response_data.clear();

    _samoa_response.set_type(core::protobuf::ERROR);
    _samoa_response.mutable_error()->set_code(err_code);
    _samoa_response.mutable_error()->set_message(err_msg);

    flush_response(std::move(guard));
}

void client_state::send_error(state::ptr_t guard,
    unsigned err_code, boost::system::error_code ec)
{
    std::stringstream tmp;
    tmp << ec << " (" << ec.message() << ")";

    send_error(std::move(guard), err_code, tmp.str());
}

void client_state::load_client_state(server::client::ptr_t client)
{
    SAMOA_ASSERT(!_client);
    _client = std::move(client);
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

void client_state::on_schedule_response(
    boost::system::error_code ec,
    server::client::response_interface iface,
    const state::ptr_t & /* guard */)
{
    if(ec)
    {
        LOG_WARN("failed to schedule response " << ec.message());
        return;
    }

    // set the response request_id to that of the request
    _samoa_response.set_request_id(_samoa_request.request_id());

    // if the response has no type, set it to that of the request
    if(!_samoa_response.has_type())
    {
        _samoa_response.set_type(_samoa_request.type());
    }

    // serlialize & queue core::protobuf::SamoaResponse for writing
    core::protobuf::zero_copy_output_adapter zco_adapter;
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

