
#include "samoa/server/client.hpp"
#include "samoa/server/context.hpp"
#include "samoa/server/protocol.hpp"
#include "samoa/server/command_handler.hpp"
#include "samoa/core/stream_protocol.hpp"
#include "samoa/error.hpp"
#include "samoa/log.hpp"
#include <boost/asio.hpp>
#include <boost/bind.hpp>

namespace samoa {
namespace server {

using namespace boost::asio;

// default timeout of 1 minute
unsigned default_timeout_ms = 60 * 1000;

client::client(context::ptr_t context, protocol::ptr_t protocol,
    core::io_service_ptr_t io_srv,
    std::unique_ptr<ip::tcp::socket> & sock)
 : core::stream_protocol(io_srv, sock),
   _context(context),
   _protocol(protocol),
   _start_called(false),
   _timeout_ms(default_timeout_ms),
   _timeout_timer(*get_io_service())
{
    LOG_DBG("created");    
}

client::~client()
{
    LOG_DBG("destroyed");
}

void client::init()
{
    on_next_request();

    // start a timeout timer, waiting for requests from the client
    _timeout_timer.expires_from_now(
        boost::posix_time::milliseconds(_timeout_ms));
    _timeout_timer.async_wait(boost::bind(
        &client::on_timeout, shared_from_this(), _1));

    _ignore_timeout = false;
}

void client::set_error(unsigned int err_code,
    const std::string & err_msg, bool closing /* = false */)
{
    _response.Clear();

    _response.set_type(core::protobuf::ERROR);
    _response.set_closing(closing);
    _response.mutable_error()->set_code(err_code);
    _response.mutable_error()->set_message(err_msg);
}

bool client::is_error_set() const
{ return _response.type() == core::protobuf::ERROR; }

void client::start_response()
{
    if(_start_called)
        return;

    SAMOA_ASSERT(!has_queued_writes());

    _start_called = true;

    // serlialize & queue core::protobuf::SamoaResponse for writing
    _response.SerializeToZeroCopyStream(&_proto_out_adapter);

    SAMOA_ASSERT(_proto_out_adapter.ByteCount() < (1<<16));

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
    SAMOA_ASSERT(_start_called);
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

void client::close()
{
    _timeout_timer.cancel();
    core::stream_protocol::close();
}

void client::on_next_request()
{
    // start request read
    read_data(boost::bind(&client::on_request_length,
        shared_from_this(), _1, _3), 2);
}

void client::on_request_length(const boost::system::error_code & ec,
    const core::buffer_regions_t & read_body)
{
    if(ec)
    {
        close();
        LOG_WARN("connection error: " << ec);
        return;
    }

    uint16_t len;
    std::copy(buffers_begin(read_body), buffers_end(read_body),
        (char*) &len);

    read_data(boost::bind(&client::on_request_body,
        shared_from_this(), _1, _3), ntohs(len));
}

void client::on_request_body(const boost::system::error_code & ec,
    const core::buffer_regions_t & read_body)
{
    if(ec)
    {
        close();
        LOG_WARN("connection error: " << ec);
        return;
    }

    _proto_in_adapter.reset(read_body);
    if(!_request.ParseFromZeroCopyStream(&_proto_in_adapter))
    {
        set_error(400, "protobuf parse error", true);
        finish_response();
        return;
    }

    // we've recieved a complete request in the timeout period
    _ignore_timeout = true;

    command_handler::ptr_t handler = _protocol->get_command_handler(
        _request.type());

    if(!handler)
    {
        set_error(501, "unknown operation type", false);
        finish_response();
        return;
    }

    // as a convienence, preset the response type
    _response.set_type(_request.type());

    // also set 'closing' if the client requested it
    if(_request.closing())
        _response.set_closing(true);

    handler->handle(shared_from_this());
    return;
}

void client::on_response_finish(const boost::system::error_code & ec)
{
    if(ec)
    {
        close();
        LOG_WARN("connection error: " << ec);
        return;
    }

    // close after writing this response?
    if(_response.closing())
    {
        close();
        return;
    }

    _start_called = false;
    _response.Clear();

    // post to begin next request
    get_io_service()->post(boost::bind(
        &client::on_next_request, shared_from_this()));
}

void client::on_timeout(boost::system::error_code ec)
{
    // _timeout_timer will call on_timeout with ec == 0 on timeout
    if(!ec && _ignore_timeout)
    {
        // start a timeout timer, waiting for requests from the client
        _timeout_timer.expires_from_now(
            boost::posix_time::milliseconds(_timeout_ms));
        _timeout_timer.async_wait(boost::bind(
            &client::on_timeout, shared_from_this(), _1));

        _ignore_timeout = false;
    }
    else if(!ec && !_ignore_timeout)
    {
        // treat this timeout as an error
        ec = boost::system::errc::make_error_code(
            boost::system::errc::stream_timeout);

        close();
        LOG_INFO("client timeout");
        return;
    }
}

}
}

