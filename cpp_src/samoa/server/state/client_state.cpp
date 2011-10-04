
#include "samoa/server/state/client_state.hpp"
#include "samoa/server/state/samoa_state.hpp"
#include "samoa/server/client.hpp"

namespace samoa {
namespace server {
namespace state {

client_state()
 : _flush_response_called(false)
{ }

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

void client_state::flush_client_response()
{
    SAMOA_ASSERT(!_flush_response_called);
    _flush_response_called = true;

    samoa_state * samoa_state = dynamic_cast<samoa_state *>(this);
    SAMOA_ASSERT(samoa_state);

    _client->schedule_response(
        boost::bind(&client_state::on_client_response,
            samoa_state->shared_from_this(), _1));
}

void client_state::send_client_error(unsigned err_code,
    const std::string & err_msg)
{
    _samoa_response.Clear();
    _response_data.clear();

    _samoa_response.set_type(core::protobuf::ERROR);
    _samoa_response.mutable_error()->set_code(err_code);
    _samoa_response.mutable_error()->set_message(err_msg);

    flush_client_response();
}

void client_state::send_client_error(unsigned err_code,
    const boost::system::error_code & ec)
{
    std::stringstream tmp;
    tmp << ec << " (" << ec.message() << ")";

    send_client_error(err_code, tmp.str());
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

void client_state::on_client_response(client::response_interface iface)
{
    // set the response request_id to that of the request
    _samoa_response.set_request_id(_samoa_request.request_id());

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
}

