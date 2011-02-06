
#include "samoa/server/client.hpp"
#include "samoa/server/context.hpp"
#include "samoa/server/protocol.hpp"
#include "samoa/server/command_handler.hpp"
#include "samoa/core/stream_protocol.hpp"
#include <boost/asio.hpp>

#include <iostream>

namespace samoa {
namespace server {

using namespace boost::asio;

client::client(context::ptr_t context, protocol::ptr_t protocol,
    std::unique_ptr<ip::tcp::socket> & sock)
 : core::stream_protocol(sock),
   _context(context),
   _protocol(protocol)
{ }

void client::init()
{
    read_data(2, boost::bind(&client::on_request_length,
        shared_from_this(), _1, _3));
}

void client::set_error(const std::string & err_type,
    const std::string & err_msg, bool closing)
{
    _response.Clear();

    _response.set_type(core::protobuf::ERROR);
    _response.mutable_error()->set_type(err_type);
    _response.mutable_error()->set_message(err_msg);
    _response.mutable_error()->set_closing(closing);
}

void client::start_response()
{
    if(_start_called)
        return;

    _start_called = true;

    if(has_queued_writes())
    {
        throw std::runtime_error("client::start_response() "
            "client already has queued writes (but shouldn't)");
    }

    // serlialize & queue core::protobuf::SamoaResponse for writing
    _response.SerializeToZeroCopyStream(&_proto_out_adapter);

    if(_proto_out_adapter.ByteCount() > (1<<16))
    {
        throw std::runtime_error("client::start_response() "
            "core::protobuf::SamoaResponse overflow (larger than 65K)");
    }

    // write network order unsigned short length
    uint16_t len = htons((uint16_t)_proto_out_adapter.ByteCount());
    queue_write((char*)&len, ((char*)&len) + 2);

    // write serialized output regions
    queue_write(_proto_out_adapter.output_regions());

    // clear state for next operation
    _proto_out_adapter.reset();
}

core::stream_protocol::write_interface_t & client::write_interface()
{
    if(!_start_called)
    {
        throw std::runtime_error(
            "client::write_interface(): start_response() hasn't been called");
    }
    return core::stream_protocol::write_interface();
}

void client::finish_response()
{
    start_response();

    // command handler may have written additional data,
    //   and already waited for those writes to finish
    if(has_queued_writes())
    {
        write_queued(boost::bind(&client::on_response_finish,
            shared_from_this(), _1));
    }
    else
    {
        on_response_finish(boost::system::error_code());
    }
    return;
}

void client::on_request_length(const boost::system::error_code & ec,
    const core::buffer_regions_t & read_body)
{
    if(ec)
    {
        std::cerr << "client::on_request_length(): ";
        std::cerr << ec.message() << std::endl;
        return;   
    }

    uint16_t len;
    std::copy(buffers_begin(read_body), buffers_end(read_body),
        (char*) &len);

    read_data(ntohs(len), boost::bind(&client::on_request_body,
        shared_from_this(), _1, _3));
}

void client::on_request_body(const boost::system::error_code & ec,
    const core::buffer_regions_t & read_body)
{
    if(ec)
    {
        std::cerr << "client::on_request_body(): ";
        std::cerr << ec.message() << std::endl;
        return;   
    }

    _proto_in_adapter.reset(read_body);
    if(!_request.ParseFromZeroCopyStream(&_proto_in_adapter))
    {
        set_error("request error", "malformed request", true);
        finish_response();
        return;
    }

    command_handler::ptr_t handler = _protocol->get_command_handler(
        _request.type());

    if(!handler)
    {
        set_error("request error", "unknown operation type", false);
        finish_response();
        return;
    }

    // as a convienence, preset the response type
    _response.set_type(_request.type());

    handler->handle(shared_from_this());
    return;
}

void client::on_response_finish(const boost::system::error_code & ec)
{
    if(ec)
    {
        std::cerr << "client::on_response_finish(): ";
        std::cerr << ec.message() << std::endl;
        return;
    }

    // close after writing this response?
    if(_response.type() == core::protobuf::ERROR &&
        _response.error().closing())
    {
        return;
    }

    // start next request
    _response.Clear();
    read_data(2, boost::bind(&client::on_request_length,
        shared_from_this(), _1, _3));
    return;
}

}
}

