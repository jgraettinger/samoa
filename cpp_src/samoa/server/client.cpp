
#include "samoa/server/client.hpp"
#include "samoa/server/context.hpp"
#include "samoa/server/protocol.hpp"
#include "samoa/server/command_handler.hpp"
#include "samoa/request/request_state.hpp"
#include "samoa/request/state_exception.hpp"
#include "samoa/core/stream_protocol.hpp"
#include "samoa/core/protobuf/zero_copy_input_adapter.hpp"
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

const unsigned client::max_request_concurrency = 100;

//////////////////////////////////////////////////////////////////////////////
//  client::response_interface

client_response_interface::client_response_interface(client::ptr_t p)
 : _client(std::move(p))
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
    write_interface().write(_client,
        boost::bind(&client::on_response_finish, _1, _2));

    // release ownership of client::response_interface
    _client.reset();
    return;
}


//////////////////////////////////////////////////////////////////////////////
//  client

client::client(context::ptr_t context, protocol::ptr_t protocol,
    std::unique_ptr<ip::tcp::socket> sock)
 : core::stream_protocol(std::move(sock)),
   _context(context),
   _protocol(protocol)
{
    LOG_DBG("");
}

client::~client()
{
    LOG_DBG("");
}

void client::initialize()
{
    on_next_request();
}

void client::on_next_request()
{
    if(_cur_requests_outstanding < client::max_request_concurrency)
    {
        ++_cur_requests_outstanding;
        _ready_for_read = false;

        read(boost::bind(&client::on_request_length, _1, _2, _3),
            shared_from_this(), 2);
    }
    else
    {
        LOG_WARN("reached maximum request concurrency; pausing read-loop");
    }
}

/* static */
void client::on_request_length(
    read_interface_t::ptr_t b_self,
    boost::system::error_code ec,
    core::buffer_regions_t read_body)
{
    ptr_t self = boost::dynamic_pointer_cast<client>(b_self);
    SAMOA_ASSERT(self);

    if(ec)
    {
        LOG_WARN(ec);
        self->on_connection_error(ec);
        // break read loop
        return;
    }

    // parse upcoming protobuf message length
    uint16_t len;
    std::copy(buffers_begin(read_body), buffers_end(read_body),
        (char*) &len);

    read_data(boost::bind(&client::on_request_body, _1, _2, _3),
        self, ntohs(len));
}

/* static */
void client::on_request_body(
    read_interface_t::ptr_t b_self,
    boost::system::error_code ec,
    core::buffer_regions_t read_body)
{
    ptr_t self = boost::dynamic_pointer_cast<client>(b_self);
    SAMOA_ASSERT(self);

    if(ec)
    {
        LOG_WARN(ec);
        self->on_connection_error(ec);
        // break read loop
        return;
    }

    request::state::ptr_t rstate = boost::make_shared<request::state>();

    request::client_state & client_state = \
        rstate->initialize_from_client(std::move(self));

    core::protobuf::zero_copy_input_adapter zci_adapter(read_body);
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

void client::schedule_response(response_callback_t callback)
{
    on_next_response(false, std::move(callback));
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

}
}

