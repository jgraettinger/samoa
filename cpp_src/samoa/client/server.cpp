
#include "samoa/client/server.hpp"
#include "samoa/core/protobuf/zero_copy_output_adapter.hpp"
#include "samoa/core/protobuf/zero_copy_input_adapter.hpp"
#include "samoa/core/connection_factory.hpp"
#include "samoa/error.hpp"
#include "samoa/log.hpp"
#include <boost/smart_ptr/make_shared.hpp>
#include <boost/bind/protect.hpp>
#include <boost/bind.hpp>

#define MAX_DATA_BLOCK_LENGTH 4194304

namespace samoa {
namespace client {

using namespace boost::asio;

// default timeout of 1 minute
unsigned default_timeout_ms = 60 * 1000;

///////////////////////////////////////////////////////////////////////////////
// Connection & construction 

//! private, friend constructor-class for use with boost::make_shared
class server_private_ctor : public server
{
public:

    server_private_ctor(const core::io_service_ptr_t & io_srv,
        std::unique_ptr<ip::tcp::socket> & sock)
     : server(io_srv, sock)
    { }
};

/* static */ core::connection_factory::ptr_t server::connect_to(
    const server_connect_to_callback_t & callback,
    const std::string & host,
    unsigned short port)
{
    return core::connection_factory::connect_to(
        boost::bind(&server::on_connect, _1, _2, _3, callback),
        host, port);
}

/* static */ void server::on_connect(
    const boost::system::error_code & ec,
    const core::io_service_ptr_t & io_srv,
    std::unique_ptr<ip::tcp::socket> & sock,
    const server_connect_to_callback_t & callback)
{
    if(ec)
    { callback(ec, ptr_t()); }
    else
    { 
        ptr_t p(boost::make_shared<server_private_ctor>(io_srv, sock));

        // start initial response read
        p->on_next_response();

        callback(ec, p);
    }
}

server::server(const core::io_service_ptr_t & io_srv,
    std::unique_ptr<boost::asio::ip::tcp::socket> & sock)
 :  core::stream_protocol(io_srv, sock),
    _next_request_id(1),
    _ready_for_write(true)
{
    LOG_DBG("created " << this);    
}

///////////////////////////////////////////////////////////////////////////////
//  server::request_interface

server_request_interface::server_request_interface()
{ }

server_request_interface::server_request_interface(const server::ptr_t & p)
 : _srv(p)
{
    if(_srv)
    {
        SAMOA_ASSERT(!_srv->has_queued_writes());
    }
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
    const core::const_buffer_regions_t & bs)
{
    size_t length = 0;

    std::for_each(bs.begin(), bs.end(),
        [&length](const core::const_buffer_region & b)
        { length += b.size(); });

    _srv->_samoa_request.add_data_block_length(length);
    _srv->_request_data.insert(_srv->_request_data.end(), bs.begin(), bs.end());
}

void server_request_interface::add_data_block(
    const core::buffer_regions_t & bs)
{
    size_t length = 0;

    std::for_each(bs.begin(), bs.end(),
        [&length](const core::buffer_region & b)
        { length += b.size(); });

    _srv->_samoa_request.add_data_block_length(length);
    _srv->_request_data.insert(_srv->_request_data.end(), bs.begin(), bs.end());
}

void server_request_interface::flush_request(
    const server::response_callback_t & callback)
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

        _srv->_pending_responses[request_id] = callback;
    }

    // begin write operation, tracked with request id
    _srv->write_queued(boost::bind(
        &server::on_request_finish, _srv, _1, request_id));

    // ownership of server::request_interface is released
    _srv.reset();
}

void server_request_interface::abort_request()
{
    _srv->on_request_finish(boost::system::error_code(), 0);

    // ownership of server::request_interface is released
    _srv.reset();
}

///////////////////////////////////////////////////////////////////////////////
//  server::response_interface

server_response_interface::server_response_interface()
{ }

server_response_interface::server_response_interface(const server::ptr_t & p)
 : _srv(p)
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
    _srv->get_io_service()->post(boost::bind(
        &server::on_next_response, _srv));

    // ownership of server::response_interface is released
    _srv.reset();
}

///////////////////////////////////////////////////////////////////////////////
//  server interface

server::~server()
{
    LOG_DBG("destroyed " << this);    
}

void server::schedule_request(const server::request_callback_t & callback)
{
    on_next_request(false, &callback);
}

void server::on_next_response()
{
    // start response read
    read_data(boost::bind(&server::on_response_length,
        shared_from_this(), _1, _3), 2);
}
    
void server::on_response_length(const boost::system::error_code & ec,
    const core::buffer_regions_t & read_body)
{
    if(ec)
    {
        on_response_error(ec);
        return;
    }

    // parse upcoming protobuf message length
    uint16_t len;
    std::copy(
        boost::asio::buffers_begin(read_body),
        boost::asio::buffers_end(read_body),
        (char*) &len);

    read_data(boost::bind(&server::on_response_body,
        shared_from_this(), _1, _3), ntohs(len));
}

void server::on_response_body(const boost::system::error_code & ec,
    const core::buffer_regions_t & read_body)
{
    if(ec)
    {
        on_response_error(ec);
        return;
    }

    core::protobuf::zero_copy_input_adapter zci_adapter(read_body);
    if(!_samoa_response.ParseFromZeroCopyStream(&zci_adapter))
    {
        // our transport is corrupted
        on_response_error(boost::system::errc::make_error_code(
            boost::system::errc::bad_message));
        return;
    }

    _response_data_blocks.clear();
    on_response_data_block(boost::system::error_code(),
        0, core::buffer_regions_t());
}

void server::on_response_data_block(const boost::system::error_code & ec,
    unsigned ind, const core::buffer_regions_t & data)
{
    if(ec)
    {
        on_response_error(ec);
        return;
    }

    if(ind < _response_data_blocks.size())
    {
        _response_data_blocks[ind] = data;
        ++ind;
    }
    else
    {
        // first call into on_response_data_block; size _response_data_blocks
        _response_data_blocks.resize(_samoa_response.data_block_length_size());
    }

    if(ind != _response_data_blocks.size())
    {
        // still more data blocks to read
        unsigned block_len = _samoa_response.data_block_length(ind);

        if(block_len > MAX_DATA_BLOCK_LENGTH)
        {
            LOG_ERR("block length larger than " << MAX_DATA_BLOCK_LENGTH);

            on_response_error(boost::system::errc::make_error_code(
                boost::system::errc::bad_message));
            return;
        }

        read_data(boost::bind(&server::on_response_data_block,
            shared_from_this(), _1, ind, _3), block_len);
        return;
    }

    unsigned request_id = _samoa_response.request_id();

    // we're done reading data blocks, and have assembled a complete resonse
    {
        spinlock::guard guard(_lock);

        pending_responses_t::iterator it = _pending_responses.find(request_id);
        SAMOA_ASSERT(it != _pending_responses.end());

        // post response interface to associated callback
        get_io_service()->post(boost::bind(it->second,
            ec, response_interface(shared_from_this())));

        _pending_responses.erase(it);
    }
}

void server::on_response_error(const boost::system::error_code & ec)
{
    // the server has closed or reset it's end of the connection;
    //  we ought to close ours immediately, as we don't want
    //  to write requests to a server which has declared
    //  it won't respond.

    close();

    // notify pending response callbacks of the error
    {
        spinlock::guard guard(_lock);

        for(auto it = _pending_responses.begin();
            it != _pending_responses.end(); ++it)
        {
            // post error to callback
            get_io_service()->post(boost::bind(it->second,
                ec, response_interface((ptr_t()))));
        }

        _pending_responses.clear();
    }
}

void server::on_next_request(bool is_write_complete,
    const request_callback_t * new_callback)
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
            _queued_request_callbacks.push_back(*new_callback);
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
            boost::bind(_queued_request_callbacks.front(),
                boost::system::error_code(),
                request_interface(shared_from_this())));

        _queued_request_callbacks.pop_front();
    }
    else if(new_callback)
    {
        _ready_for_write = false;

        // post call directly to new_callback
        get_io_service()->post(boost::bind(*new_callback,
            boost::system::error_code(),
            request_interface(shared_from_this())));
    }
    // else, no request to begin at this point
}

void server::on_request_finish(const boost::system::error_code & ec,
    unsigned request_id)
{
    if(ec)
    {
        spinlock::guard guard(_lock);

        pending_responses_t::iterator it = _pending_responses.find(request_id);

        // race condition check: on_response_error may have already
        //   posted an error & cleared the callback
        if(it != _pending_responses.end())
        {
            // post error to response callback
            get_io_service()->post(boost::bind(it->second,
                ec, response_interface((ptr_t()))));

            _pending_responses.erase(it);
        }
    }

    // start a new request, if there is one
    on_next_request(true, 0);
}

}
}

