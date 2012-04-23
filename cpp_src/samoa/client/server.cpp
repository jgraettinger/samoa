
#include "samoa/client/server.hpp"
#include "samoa/core/protobuf/zero_copy_output_adapter.hpp"
#include "samoa/core/protobuf/zero_copy_input_adapter.hpp"
#include "samoa/core/connection_factory.hpp"
#include "samoa/error.hpp"
#include "samoa/log.hpp"
#include <boost/smart_ptr/make_shared.hpp>
#include <functional>

namespace samoa {
namespace client {

using namespace boost::asio;

///////////////////////////////////////////////////////////////////////////////
//  server::request_interface

server_request_interface::server_request_interface(server::ptr_t p)
 : _srv(std::move(p))
{
    SAMOA_ASSERT(_srv && !_srv->has_queued_writes());
}

core::protobuf::SamoaRequest & server_request_interface::get_message() const
{
    SAMOA_ASSERT(_srv);
    return _srv->_samoa_request;
}

void server_request_interface::add_data_block(
    const core::buffer_region & b) const
{
    SAMOA_ASSERT(_srv);
    _srv->_samoa_request.add_data_block_length(b.size());
    _srv->_request_data.push_back(b);
}

void server_request_interface::add_data_block(
    const core::buffer_regions_t & bs) const
{
    SAMOA_ASSERT(_srv);

    size_t length = 0;

    std::for_each(bs.begin(), bs.end(),
        [&length](const core::buffer_region & b)
        { length += b.size(); });

    _srv->_samoa_request.add_data_block_length(length);
    _srv->_request_data.insert(_srv->_request_data.end(),
           bs.begin(), bs.end());
}

void server_request_interface::flush_request(
    server::response_callback_t callback)
{
    SAMOA_ASSERT(_srv);

    _srv->get_io_service()->dispatch(
        [self = _srv, callback = std::move(callback)]()
        {
            server::on_flush_request(std::move(self), std::move(callback));
        });

    // ownership of server::request_interface is released
    _srv.reset();
}

void server_request_interface::abort_request()
{
    SAMOA_ASSERT(_srv);

    _srv->get_io_service()->post(
        std::bind(&server::on_request_finish,
            _srv, boost::system::error_code()));

    // ownership of server::request_interface is released
    _srv.reset();
}

///////////////////////////////////////////////////////////////////////////////
//  server::response_interface

server_response_interface::server_response_interface(server::ptr_t p)
 : _srv(std::move(p))
{ }

const core::protobuf::SamoaResponse &
server_response_interface::get_message() const
{
    SAMOA_ASSERT(_srv);
    return _srv->_samoa_response;
}

unsigned server_response_interface::get_error_code() const
{
    const core::protobuf::SamoaResponse & resp = get_message();
    if(resp.type() == core::protobuf::ERROR)
    {
        return resp.error().code();
    }
    return 0;
}

const std::vector<core::buffer_regions_t> &
    server_response_interface::get_response_data_blocks() const
{
    SAMOA_ASSERT(_srv);
    return _srv->_response_data_blocks;
}

void server_response_interface::finish_response()
{
    SAMOA_ASSERT(_srv);

    _srv->get_io_service()->post(std::bind(&server::on_next_response, _srv));

    // ownership of server::response_interface is released
    _srv.reset();
}

///////////////////////////////////////////////////////////////////////////////
//  server interface

server::server(std::unique_ptr<ip::tcp::socket> sock,
    core::io_service_ptr_t io_srv)
 :  core::stream_protocol(std::move(sock), std::move(io_srv)),
    _next_request_id(1),
    _ready_for_write(1)
{
    LOG_DBG("created " << this);    
}

/* static */
void server::connect_to(
    server_connect_to_callback_t callback,
    const std::string & host,
    unsigned short port)
{
    core::connection_factory::connect_to(
        std::bind(&server::on_connect,
            std::placeholders::_1,
            std::placeholders::_2,
            std::placeholders::_3,
            std::move(callback)),
        host, port);
}

/* static */
void server::on_connect(
    boost::system::error_code ec,
    std::unique_ptr<ip::tcp::socket> sock,
    core::io_service_ptr_t io_srv,
    server_connect_to_callback_t callback)
{
    if(ec)
    {
        callback(ec, ptr_t());
    }
    else
    { 
        ptr_t self(boost::make_shared<server>(
            std::move(sock), std::move(io_srv)));

        // start initial response read
        self->on_next_response();

        callback(ec, std::move(self));
    }
}

server::~server()
{
    LOG_DBG("destroyed " << this);    
}

void server::schedule_request(request_callback_t callback)
{
    get_io_service()->dispatch(
        [&, self = shared_from_this(), callback = std::move(callback)]()
        {
            // '&' capture is a work-around for a bug in gcc 4.6; remove me
            server::on_schedule_request(std::move(self), std::move(callback));
        });
}

/* static */
void server::on_schedule_request(ptr_t self, request_callback_t callback)
{
    // we should never be write-ready if there are queued requests
    SAMOA_ASSERT(!self->_ready_for_write || self->_pending_requests.empty());

    if(self->_ready_for_write)
    {
        // dispatch immediately
        self->_ready_for_write = false;

        callback(boost::system::error_code(),
            request_interface(std::move(self)));
        return;
    }
    else
    {
        // wrap callback with a closure which guards server lifetime
        auto wrapped_callback = [
            self = self,
            callback = std::move(callback)
        ](boost::system::error_code ec)
        {
            if(ec)
            {
                callback(ec, request_interface());
            }
            else
            {
                callback(boost::system::error_code(),
                    request_interface(std::move(self)));
            }
        };

        self->_pending_requests.push_back(std::move(wrapped_callback));
    }
}

void server::on_flush_request(ptr_t self, response_callback_t callback)
{
    // precondition:
    //
    // _samoa_request & _request_data have been fully populated
    //  by a server_request_interface instance. we bundle the request
    //  and kick off a socket write, and queue the callback as a pending
    //  response under the generated request_id

    // generate & set a new request_id
    unsigned request_id = self->_next_request_id++;
    self->_samoa_request.set_request_id(request_id);

    // serialize SamoaRequest & reset
    core::protobuf::zero_copy_output_adapter zco_adapter;
    self->_samoa_request.SerializeToZeroCopyStream(&zco_adapter);

    SAMOA_ASSERT(zco_adapter.ByteCount() < (1<<16));

    // write network order unsigned short length
    uint16_t len = htons((uint16_t)zco_adapter.ByteCount());
    self->queue_write((char*)&len, ((char*)&len) + 2);

    // queue write of serialized output regions
    self->queue_write(zco_adapter.output_regions());

    // queue spooled request data
    self->queue_write(self->_request_data);

    // kick off socket write
    self->write(
        std::bind(&server::on_request_finish,
            std::placeholders::_1, std::placeholders::_2),
        self);

    server * raw_self = self.get();

    // move self & callback into a closure which guards server lifetime
    auto wrapped_callback = [
        self = std::move(self),
        callback = std::move(callback)
    ](boost::system::error_code ec)
    {
        if(ec)
        {
            callback(ec, response_interface());
        }
        else
        {
            callback(boost::system::error_code(),
                response_interface(std::move(self)));
        }
    };

    // index callback on request_id
    SAMOA_ASSERT(raw_self->_pending_responses.insert(
        std::make_pair(request_id, std::move(wrapped_callback))).second);
}

/* static */
void server::on_request_finish(
    write_interface_t::ptr_t b_self,
    boost::system::error_code ec)
{
    ptr_t self = boost::dynamic_pointer_cast<server>(b_self);
    SAMOA_ASSERT(self);

    // this needs to be true, even if the socket's closed:
    //  we want requests which are currently getting scheduled to attempt
    //  a write and fail, rather than get stuck in _pending_requests
    self->_ready_for_write = true;

    // reset for next use
    self->_samoa_request.Clear();
    self->_request_data.clear();

    if(ec)
    {
        LOG_WARN(ec.message());
        self->on_connection_error(ec);
        return;
    }

    if(!self->_pending_requests.empty())
    {
        self->_ready_for_write = false;

        // dequeue next request, and callback
        wrapped_callback_t callback = std::move(
            self->_pending_requests.front());
        self->_pending_requests.pop_front();

        callback(boost::system::error_code());
    }
}

void server::on_next_response()
{
    // begin read of response message length
    read(std::bind(&server::on_response_length,
            std::placeholders::_1,
            std::placeholders::_2,
            std::placeholders::_3),
        shared_from_this(), 2);
}

/* static */
void server::on_response_length(
    read_interface_t::ptr_t b_self,
    boost::system::error_code ec,
    core::buffer_regions_t read_body)
{
    ptr_t self = boost::dynamic_pointer_cast<server>(b_self);
    SAMOA_ASSERT(self);

    if(ec)
    {
        LOG_WARN(ec.message());
        self->on_connection_error(ec);
        return;
    }

    // parse upcoming protobuf message length
    uint16_t len;
    std::copy(buffers_begin(read_body), buffers_end(read_body),
        (char*) &len);

    self->read(std::bind(&server::on_response_body,
            std::placeholders::_1,
            std::placeholders::_2,
            std::placeholders::_3),
        self, ntohs(len));
}

/* static */
void server::on_response_body(
    read_interface_t::ptr_t b_self,
    boost::system::error_code ec,
    core::buffer_regions_t read_body)
{
    ptr_t self = boost::dynamic_pointer_cast<server>(b_self);
    SAMOA_ASSERT(self);

    if(ec)
    {
        LOG_WARN(ec.message());
        self->on_connection_error(ec);
        return;
    }

    core::protobuf::zero_copy_input_adapter zci_adapter(read_body);
    if(!self->_samoa_response.ParseFromZeroCopyStream(&zci_adapter))
    {
        // our transport is corrupted
        ec = boost::system::errc::make_error_code(
            boost::system::errc::bad_message);

        LOG_WARN(ec.message());
        self->on_connection_error(ec);
        // break read loop
        return;
    }

    self->_response_data_blocks.clear();

    on_response_data_block(
        std::move(b_self),
        boost::system::error_code(),
        core::buffer_regions_t(),
        0);
}

void server::on_response_data_block(
    read_interface_t::ptr_t b_self,
    boost::system::error_code ec,
    core::buffer_regions_t data,
    unsigned ind)
{
    ptr_t self = boost::dynamic_pointer_cast<server>(b_self);
    SAMOA_ASSERT(self);

    if(ec)
    {
        LOG_WARN(ec.message());
        self->on_connection_error(ec);
        return;
    }

    if(ind < self->_response_data_blocks.size())
    {
        self->_response_data_blocks[ind] = data;
        ++ind;
    }
    else
    {
        // first call into on_response_data_block; size _response_data_blocks
        self->_response_data_blocks.resize(
            self->_samoa_response.data_block_length_size());
    }

    if(ind != self->_response_data_blocks.size())
    {
        // still more data blocks to read
        unsigned next_length = self->_samoa_response.data_block_length(ind);

        self->read(std::bind(&server::on_response_data_block,
                std::placeholders::_1,
                std::placeholders::_2,
                std::placeholders::_3,
                ind),
            self, next_length);
        return;
    }
    // we're done reading data blocks, and have assembled a complete resonse

    unsigned request_id = self->_samoa_response.request_id();

    // query for response callback
    auto it = self->_pending_responses.find(request_id);

    if(it == std::end(self->_pending_responses))
    {
        LOG_WARN("received response with an unknown request_id " \
            << request_id << " from " << self->get_remote_address() \
            << ":" << self->get_remote_port());

        ec = boost::system::errc::make_error_code(
            boost::system::errc::bad_message);
        self->on_connection_error(ec);
        return;
    }
    else
    {
        // move out of pending responses, & erase
        wrapped_callback_t callback = std::move(it->second);
        self->_pending_responses.erase(it);

        callback(ec);
    }
}

void server::on_connection_error(boost::system::error_code ec)
{
    SAMOA_ASSERT(ec);

    if(is_open())
    {
        get_socket().close();
    }

    // notify pending response callbacks of the error
    for(auto & entry : _pending_responses)
    {
        // post deferred error callback
        get_io_service()->post([ec, callback = std::move(entry.second)]()
            {
                callback(ec);
            });
    }
    _pending_responses.clear();

    // notify pending request callbacks of the error
    for(auto & callback : _pending_requests)
    {
        get_io_service()->post([ec, callback = std::move(callback)]()
            {
                callback(ec);
            });
    }
    _pending_requests.clear();
}

}
}

