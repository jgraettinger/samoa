
#include "samoa/client/server.hpp"
#include "samoa/core/protobuf/zero_copy_output_adapter.hpp"
#include "samoa/core/protobuf/zero_copy_input_adapter.hpp"
#include "samoa/core/connection_factory.hpp"
#include "samoa/error.hpp"
#include "samoa/log.hpp"
#include <boost/smart_ptr/make_shared.hpp>
#include <boost/bind/protect.hpp>
#include <boost/bind.hpp>

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

core::protobuf::SamoaRequest & server_request_interface::get_message()
{ return _srv->_samoa_request; }

void server_request_interface::add_data_block(
    const core::buffer_region & b)
{
    _srv->_samoa_request.add_data_block_length(b.size());
    _srv->_request_data.push_back(b);
}

void server_request_interface::add_data_block(
    const core::buffer_regions_t & bs)
{
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
    // generate & set a new request_id
    unsigned request_id = _srv->_next_request_id++;
    _srv->_samoa_request.set_request_id(request_id);

    // serialize & reset SamoaRequest
    core::protobuf::zero_copy_output_adapter zco_adapter;
    _srv->_samoa_request.SerializeToZeroCopyStream(&zco_adapter);
    _srv->_samoa_request.Clear();

    SAMOA_ASSERT(zco_adapter.ByteCount() < (1<<16));

    // write network order unsigned short length
    uint16_t len = htons((uint16_t)zco_adapter.ByteCount());
    _srv->queue_write((char*)&len, ((char*)&len) + 2);

    // queue write of serialized output regions
    _srv->queue_write(zco_adapter.output_regions());

    // queue spooled request data
    _srv->queue_write(_srv->_request_data);
    _srv->_request_data.clear();

    // index callback under this request_id
    {
        spinlock::guard guard(_srv->_lock);

        SAMOA_ASSERT(_srv->_pending_responses.insert(
            std::make_pair(request_id, std::move(callback))).second);
    }

    // begin write operation, tracked with request id
    _srv->write(_srv, boost::bind(&server::on_request_finish,
        _1, _2));

    // ownership of server::request_interface is released
    _srv.reset();
}

void server_request_interface::abort_request()
{
    server::on_request_finish(_srv, boost::system::error_code());

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
{ return _srv->_samoa_response; }

unsigned server_response_interface::get_error_code()
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
    return _srv->_response_data_blocks;
}

void server_response_interface::finish_response()
{
    // post next response read
    _srv->get_io_service().post(boost::bind(
        &server::on_next_response, _srv));

    // ownership of server::response_interface is released
    _srv.reset();
}

///////////////////////////////////////////////////////////////////////////////
//  server interface

server::server(std::unique_ptr<ip::tcp::socket> sock)
 :  core::stream_protocol(std::move(sock))
{
    LOG_DBG("created " << this);    
}

/* static */
core::connection_factory::ptr_t server::connect_to(
    server_connect_to_callback_t callback,
    std::string host,
    unsigned short port)
{
    return core::connection_factory::connect_to(
        boost::bind(&server::on_connect, _1, _3, std::move(callback)),
        std::move(host), port);
}

/* static */
void server::on_connect(
    boost::system::error_code ec,
    std::unique_ptr<ip::tcp::socket> sock,
    server_connect_to_callback_t callback)
{
    if(ec)
    {
        callback(ec, ptr_t());
    }
    else
    { 
        ptr_t p(boost::make_shared<server>(std::move(sock)));

        // start initial response read
        p->on_next_response();

        callback(ec, std::move(p));
    }
}

server::~server()
{
    LOG_DBG("destroyed " << this);    
}

void server::on_next_response()
{
    // begin read of response message length
    read(boost::bind(&server::on_response_length, _1, _2, _3),
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
        LOG_WARN(ec);
        self->on_connection_error(ec);
        // break read loop
        return;
    }

    // parse upcoming protobuf message length
    uint16_t len;
    std::copy(buffers_begin(read_body), buffers_end(read_body),
        (char*) &len);

    read(boost::bind(&server::on_response_body, _1, _2, _3),
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
        LOG_WARN(ec);
        self->on_connection_error(ec);
        // break read loop
        return;
    }

    core::protobuf::zero_copy_input_adapter zci_adapter(read_body);
    if(!self->_samoa_response.ParseFromZeroCopyStream(&zci_adapter))
    {
        // our transport is corrupted
        ec = boost::system::errc::make_error_code(
            boost::system::errc::bad_message);

        LOG_WARN(ec);
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
        LOG_WARN(ec);
        self->on_connection_error(ec);
        // break read loop
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

        read(boost::bind(&server::on_response_data_block, _1, _2, _3, ind),
            self, next_length);
        return;
    }
    // we're done reading data blocks, and have assembled a complete resonse

    unsigned request_id = self->_samoa_response.request_id();

    response_callback_t callback;
    {
        spinlock::guard guard(self->_lock);

        // query for response callback
        auto it = self->_pending_responses.find(request_id);
        SAMOA_ASSERT(it != std::end(self->_pending_responses));

        // move out of pending responses, & erase
        callback = std::move(it->second);
        _pending_responses.erase(it);
    }

    // post response interface to associated callback
    callback(ec, response_interface(std::move(self)));
}

void server::on_connection_error(boost::system::error_code ec)
{
    close();

    // notify pending response callbacks of the error
    {
        spinlock::guard guard(_lock);

        for(auto it = _pending_responses.begin();
            it != _pending_responses.end(); ++it)
        {
            // post error to callback
            get_io_service()->post(boost::bind(it->second,
                ec, response_interface()));
        }
        _pending_responses.clear();
    }
}

void server::schedule_request(server::request_callback_t callback)
{
    on_next_request(false, &callback);
}

void server::on_next_request(bool is_write_complete,
    request_callback_t * new_callback)
{
    spinlock::guard guard(_lock);

    SAMOA_ASSERT(is_write_complete ^ (new_callback != 0));

    if(is_write_complete)
    {
        _ready_for_write = true;
    }

    if(!_ready_for_write)
    {
        if(new_callback)
        {
            _queued_request_callbacks.push_back(std::move(*new_callback));
        }
        return;
    }

    // we're ready to start a new request

    if(!_queued_request_callbacks.empty())
    {
        // it should never be possible for us to be ready-to-write,
        //  and have both a new_callback & queued callbacks.
        //
        // only on_request_finish can clear the ready_for_write bit,
        //  and that call never issues a new_callback
        SAMOA_ASSERT(!new_callback);

        _ready_for_write = false;

        // post call to next queued request callback
        get_io_service()->post(
            boost::bind(std::move(_queued_request_callbacks.front()),
                boost::system::error_code(),
                request_interface(shared_from_this())));

        _queued_request_callbacks.pop_front();
    }
    else if(new_callback)
    {
        _ready_for_write = false;

        // post call directly to new_callback
        get_io_service()->post(boost::bind(std::move(*new_callback),
            boost::system::error_code(),
            request_interface(shared_from_this())));
    }
    // else, no request to begin at this point
}

void server::on_request_finish(
    write_interface_t::ptr_t b_self,
    const boost::system::error_code & ec)
{
    ptr_t self = boost::dynamic_pointer_cast<server>(b_self);
    SAMOA_ASSERT(self);

    if(ec)
    {
        on_connection_error(ec);
        // break write loop
        return;
    }

    // start a new request, if there is one
    self->on_next_request(true, nullptr);
}

}
}

