
#include "samoa/server/client.hpp"
#include "samoa/server/context.hpp"
#include "samoa/server/protocol.hpp"
#include "samoa/server/command_handler.hpp"
#include "samoa/request/request_state.hpp"
#include "samoa/request/state_exception.hpp"
#include "samoa/core/stream_protocol.hpp"
#include "samoa/error.hpp"
#include "samoa/log.hpp"
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>
#include <sstream>

#define MAX_DATA_BLOCK_LENGTH 4194304

namespace samoa {
namespace server {

using namespace boost::asio;

// default timeout of 1 minute
unsigned default_timeout_ms = 10 * 1000;

const unsigned client::max_request_concurrency = 100;

//////////////////////////////////////////////////////////////////////////////
//  client::response_interface

client_response_interface::client_response_interface(const client::ptr_t & p)
 : _client(p)
{
    SAMOA_ASSERT(_client && !_client->has_queued_writes())
}

core::stream_protocol::write_interface_t &
client_response_interface::write_interface()
{
    return _client->write_interface();
}

void client_response_interface::finish_response()
{
    write_interface().write_queued(
        boost::bind(&client::on_response_finish, _client, _1));

    // release ownership of client::response_interface
    _client.reset();
    return;
}


//////////////////////////////////////////////////////////////////////////////
//  client

client::client(context::ptr_t context, protocol::ptr_t protocol,
    core::io_service_ptr_t io_srv,
    std::unique_ptr<ip::tcp::socket> & sock)
 : core::stream_protocol(io_srv, sock),
   _context(context),
   _protocol(protocol),
   _ready_for_read(true),
   _ready_for_write(true),
   _cur_requests_outstanding(0),
   _ignore_timeout(false),
   _timeout_ms(default_timeout_ms),
   _timeout_timer(*get_io_service())
{
    LOG_DBG("");
}

client::~client()
{
	_context->drop_client(reinterpret_cast<size_t>(this));
    LOG_DBG("");
}

void client::initialize()
{
	_context->add_client(reinterpret_cast<size_t>(this),
        shared_from_this());

    on_next_request();

    // start a timeout timer, waiting for requests from the client
    _timeout_timer.expires_from_now(
        boost::posix_time::milliseconds(_timeout_ms));
    _timeout_timer.async_wait(boost::bind(
        &client::on_timeout, shared_from_this(), _1));
}

void client::shutdown()
{
    core::stream_protocol::close();
    _timeout_timer.cancel();
}

void client::schedule_response(const response_callback_t & callback)
{
    on_next_response(false, &callback);
}

void client::on_next_request()
{
    if(_cur_requests_outstanding < client::max_request_concurrency)
    {
        ++_cur_requests_outstanding;
        _ready_for_read = false;

        read_data(boost::bind(&client::on_request_length,
            shared_from_this(), _1, _3), 2);
    }
    else
    {
        LOG_WARN("reached maximum request concurrency; pausing read-loop");
    }
}

void client::on_request_length(const boost::system::error_code & ec,
    const core::buffer_regions_t & read_body)
{
    if(ec)
    {
        LOG_WARN(ec.message());
        _timeout_timer.cancel();
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
        LOG_WARN(ec.message());
        _timeout_timer.cancel();
        return;
    }

    request::state::ptr_t rstate = boost::make_shared<request::state>();

    request::client_state & client_state = \
        rstate->initialize_from_client(shared_from_this());

    core::zero_copy_input_adapter zci_adapter(read_body);
    if(!client_state.mutable_samoa_request(
        ).ParseFromZeroCopyStream(&zci_adapter))
    {
        rstate->send_error(400, "protobuf parse error");
        // our transport is corrupted: don't begin a new read
        //  this client will be destroyed when remaining responses
        //  complete, & the connection will close
        return;
    }

    on_request_data_block(boost::system::error_code(),
        0, core::buffer_regions_t(), rstate,
        client_state.mutable_request_data_blocks());
}

void client::on_request_data_block(const boost::system::error_code & ec,
    unsigned ind, const core::buffer_regions_t & data,
    const request::state::ptr_t & rstate,
    std::vector<core::buffer_regions_t> & data_blocks)
{
    if(ec)
    {
        LOG_WARN(ec.message());
        _timeout_timer.cancel();
        return;
    }

    const spb::SamoaRequest & samoa_request = rstate->get_samoa_request();

    if(ind < data_blocks.size())
    {
        data_blocks[ind] = data;
        ++ind;
    }
    else
    {
        // this is first entrance into on_request_data_block();
        //   resize data_blocks appropriately
        data_blocks.resize(samoa_request.data_block_length_size());
    }

    if(ind != data_blocks.size())
    {
        // still more data blocks to read
        unsigned block_len = samoa_request.data_block_length(ind);

        if(block_len > MAX_DATA_BLOCK_LENGTH)
        {
            rstate->send_error(400, "data block too large");
            // our transport is corrupted: don't begin a new read
            return;
        }

        read_data(
            boost::bind(&client::on_request_data_block, shared_from_this(),
                _1, ind, _3, rstate, boost::ref(data_blocks)),
            block_len);
        return;
    }

    // we're done reading data blocks, and have recieved a 
    // complete request in the timeout period
    _ignore_timeout = true;
    _ready_for_read = true;

    // post to continue request read-loop
    get_io_service()->post(boost::bind(
        &client::on_next_request, shared_from_this()));

    command_handler::ptr_t handler = _protocol->get_command_handler(
        samoa_request.type());

    if(handler)
    {
        handler->checked_handle(rstate);
    }
    else
    {
        rstate->send_error(501, "unknown operation type");
    }
}

void client::on_next_response(bool is_write_complete,
    const response_callback_t * new_callback)
{
    SAMOA_ASSERT(is_write_complete ^ (new_callback != 0));

    spinlock::guard guard(_lock);

    if(is_write_complete)
    {
        _ready_for_write = true;

    }

    if(!_ready_for_write)
    {
        if(new_callback)
        {
            _queued_response_callbacks.push_back(*new_callback);
        }
        return;
    }

    // we're ready to start a new response

    if(!_queued_response_callbacks.empty())
    {
        // it should never be possible for us to be ready-to-write,
        //  and have both a new_callback & queued callbacks.
        //
        // only on_response_finish can clear the ready_for_write bit,
        //  and that call never issues a new_callback
        SAMOA_ASSERT(!new_callback);

        _ready_for_write = false;

        // post call to next queued response callback
        get_io_service()->post(boost::bind(_queued_response_callbacks.front(),
            response_interface(shared_from_this())));

        _queued_response_callbacks.pop_front();
    }
    else if(new_callback)
    {
        _ready_for_write = false;

        // post call directly to new_callback
        get_io_service()->post(boost::bind(*new_callback,
            response_interface(shared_from_this())));
    }
    // else, no response to begin at this point
}

void client::on_response_finish(const boost::system::error_code & ec)
{
    if(ec)
    {
        LOG_WARN(ec.message());
    }

    if(_ready_for_read &&
       _cur_requests_outstanding == client::max_request_concurrency)
    {
        // this response caused us to drop back below maximum
        //  request concurrency; restart the request read-loop via post
        get_io_service()->post(boost::bind(&client::on_next_request,
            shared_from_this()));

        LOG_INFO("request concurrency dropped; restarting read-loop");
    }
    --_cur_requests_outstanding;

    on_next_response(true, 0);
}

void client::on_timeout(boost::system::error_code ec)
{
    if(ec)
    {
        // timer cancelled; ignore
        return;
    }

    if(_ignore_timeout || _cur_requests_outstanding > 1)
    {
        // we've recently receieved a request from this client,
        //  or they have pending responses still in flight

        // start a timeout timer, waiting for requests from the client
        _timeout_timer.expires_from_now(
            boost::posix_time::milliseconds(_timeout_ms));
        _timeout_timer.async_wait(boost::bind(
            &client::on_timeout, shared_from_this(), _1));

        _ignore_timeout = false;
    }
    else
    {
        LOG_INFO("client timeout");
        core::stream_protocol::close();
    }
}

}
}

