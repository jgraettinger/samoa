
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
#include <functional>

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
    SAMOA_ASSERT(_client);
    return *_client;
}

void client_response_interface::finish_response()
{
    SAMOA_ASSERT(_client);

    client * raw_client = _client.get();

    // dispatch write in io_service's thread;
    raw_client->get_io_service()->dispatch(
        [self = std::move(_client)]()
        {
            self->write(
                std::bind(&client::on_response_finish,
                    std::placeholders::_1, std::placeholders::_2),
                self);
        });

    // move of client released ownership of client::response_interface
    SAMOA_ASSERT(!_client);
}


//////////////////////////////////////////////////////////////////////////////
//  client

client::client(context::ptr_t context, protocol::ptr_t protocol,
    std::unique_ptr<ip::tcp::socket> sock,
    core::io_service_ptr_t io_srv)
 :  core::stream_protocol(std::move(sock), std::move(io_srv)),
    _context(context),
    _protocol(protocol),
    _ready_for_read(true),
    _ready_for_write(true),
    _cur_requests_outstanding(0)
{
    LOG_DBG("created " << this << " owning " << &get_socket());
}

client::~client()
{
    LOG_DBG("destroyed " << this << " owning " << &get_socket());
}

void client::initialize()
{
    // begin client read loop
    get_io_service()->dispatch(
        std::bind(&client::on_next_request, shared_from_this()));
}

void client::on_next_request()
{
    if(_cur_requests_outstanding < client::max_request_concurrency)
    {
        ++_cur_requests_outstanding;
        _ready_for_read = false;

        read(std::bind(&client::on_request_length,
                std::placeholders::_1,
                std::placeholders::_2,
                std::placeholders::_3),
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
        return;
    }

    // parse upcoming protobuf message length
    uint16_t len;
    std::copy(buffers_begin(read_body), buffers_end(read_body),
        (char*) &len);

    self->read(std::bind(&client::on_request_body,
            std::placeholders::_1,
            std::placeholders::_2,
            std::placeholders::_3),
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
        return;
    }

    self->_next_rstate = boost::make_shared<request::state>();
    request::client_state & client_state = \
        self->_next_rstate->mutable_client_state();

    core::protobuf::zero_copy_input_adapter zci_adapter(read_body);
    if(!client_state.mutable_samoa_request(
        ).ParseFromZeroCopyStream(&zci_adapter))
    {
        // our transport is corrupted: don't begin a new read.
        //  this client will be destroyed when remaining responses
        //  complete, & the connection will close

        request::state_ptr_t rstate = std::move(self->_next_rstate);
        rstate->initialize_from_client(std::move(self));
        rstate->send_error(400, "protobuf parse error");
        return;
    }

    on_request_data_block(
        std::move(b_self),
        boost::system::error_code(),
        core::buffer_regions_t(),
        0);
}

/* static */
void client::on_request_data_block(
    read_interface_t::ptr_t b_self,
    boost::system::error_code ec,
    core::buffer_regions_t data,
    unsigned ind)
{
    ptr_t self = boost::dynamic_pointer_cast<client>(b_self);
    SAMOA_ASSERT(self);

    if(ec)
    {
        LOG_WARN(ec.message());
        self->on_connection_error(ec);
        return;
    }

    const spb::SamoaRequest & samoa_request = \
        self->_next_rstate->get_samoa_request();

    std::vector<core::buffer_regions_t> & data_blocks = \
        self->_next_rstate->mutable_client_state(
                ).mutable_request_data_blocks();

    if(ind < data_blocks.size())
    {
        data_blocks[ind] = std::move(data);
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
        unsigned next_length = samoa_request.data_block_length(ind);

        self->read(std::bind(&client::on_request_data_block,
                std::placeholders::_1,
                std::placeholders::_2,
                std::placeholders::_3,
                ind),
            self, next_length);
        return;
    }

    // we're done reading data blocks, and have recieved a complete request
    self->_ready_for_read = true;

    // post to continue request read-loop
    self->get_io_service()->post(std::bind(
        &client::on_next_request, self));

    command_handler::ptr_t handler = self->_protocol->get_command_handler(
        samoa_request.type());

    // release _next_state & self references
    request::state::ptr_t rstate = std::move(self->_next_rstate);
    rstate->initialize_from_client(std::move(self));

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
    get_io_service()->dispatch(
        [&, self = shared_from_this(), callback = std::move(callback)]()
        {
            // '&' capture is a work-around for a bug in gcc 4.6; remove me
            client::on_schedule_response(std::move(self), std::move(callback));
        });
}

/* static */
void client::on_schedule_response(ptr_t self, response_callback_t callback)
{
    // we should never be write-ready if there are queued responses
    SAMOA_ASSERT(!self->_ready_for_write || self->_pending_responses.empty());

    if(self->_ready_for_write)
    {
        // dispatch immediately
        self->_ready_for_write = false;

        callback(boost::system::error_code(),
            response_interface(std::move(self)));
        return;
    }
    else
    {
        self->_pending_responses.push_back(std::move(callback));
    }
}

/* static */
void client::on_response_finish(
    write_interface_t::ptr_t b_self,
    boost::system::error_code ec)
{
    ptr_t self = boost::dynamic_pointer_cast<client>(b_self);
    SAMOA_ASSERT(self);

    // this needs to be true, even if the socket's closed:
    //  we want responses which are currently getting scheduled to attempt
    //  a write and fail, rather than get stuck in _pending_responses
    self->_ready_for_write = true;

    if(ec)
    {
        LOG_WARN(ec.message());
        self->on_connection_error(ec);
        return;
    }

    if(self->_ready_for_read &&
       self->_cur_requests_outstanding == client::max_request_concurrency)
    {
        // this response caused us to drop back below maximum
        //  request concurrency; restart the request read-loop via post
        self->get_io_service()->post(
            std::bind(&client::on_next_request, self));

        LOG_INFO("request concurrency dropped; restarting read-loop");
    }
    --self->_cur_requests_outstanding;

    if(!self->_pending_responses.empty())
    {
        // begin another response
        self->_ready_for_write = false;

        response_callback_t callback = std::move(
            self->_pending_responses.front());
        self->_pending_responses.pop_front();

        callback(boost::system::error_code(),
            response_interface(std::move(self)));
        return;
    }
}

void client::on_connection_error(boost::system::error_code ec)
{
    SAMOA_ASSERT(ec);

    if(is_open())
    {
        get_socket().close();
        _context->drop_client(reinterpret_cast<size_t>(this));
    }

    // notify pending response callbacks of the error
    for(auto & callback : _pending_responses)
    {
        // post deferred error callback
        get_io_service()->post([ec, callback = std::move(callback)]()
            {
                callback(ec, response_interface());
            });
    }
    _pending_responses.clear();
}

}
}

