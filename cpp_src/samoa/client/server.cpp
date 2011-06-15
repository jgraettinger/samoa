
#include "samoa/client/server.hpp"
#include "samoa/core/connection_factory.hpp"
#include "samoa/log.hpp"
#include <boost/smart_ptr/make_shared.hpp>
#include <boost/bind/protect.hpp>
#include <boost/bind.hpp>

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
    _in_request(false),
    _start_request_called(false),
    _queue_size(0),
    _ignore_timeout(false),
    _timeout_ms(default_timeout_ms),
    _timeout_timer(*get_io_service())
{
    LOG_DBG("created");
}

///////////////////////////////////////////////////////////////////////////////
//  server::request_interface

server_request_interface::server_request_interface(const server::ptr_t & p)
 : _srv(p)
{
    if(_srv)
    {
        assert(!_srv->has_queued_writes());
    }
}

core::protobuf::SamoaRequest & server_request_interface::get_message()
{ return _srv->_request; }

void server_request_interface::start_request()
{
    if(_srv->_start_request_called)
        return;

    _srv->_start_request_called = true;

    if(_srv->has_queued_writes())
    {
        throw std::runtime_error(
            "server::request_interface::start_request(): "
            "server already has queued writes (but shouldn't)");
    }

    // serlialize & queue core::protobuf::SamoaRequest for writing
    _srv->_request.SerializeToZeroCopyStream(&_srv->_proto_out_adapter);

    if(_srv->_proto_out_adapter.ByteCount() > (1<<16))
    {
        throw std::runtime_error(
            "server::request_interface::start_request(): "
            "core::protobuf::SamoaRequest overflow (larger than 65K)");
    }

    // write network order unsigned short length
    uint16_t len = htons((uint16_t)_srv->_proto_out_adapter.ByteCount());
    _srv->queue_write((char*)&len, ((char*)&len) + 2);

    // queue write of serialized output regions
    _srv->queue_write(_srv->_proto_out_adapter.output_regions());

    // clear state for next operation
    _srv->_proto_out_adapter.reset();
}

core::stream_protocol::write_interface_t &
    server_request_interface::write_interface()
{
    if(!_srv->_start_request_called)
    {
        throw std::runtime_error(
            "server::request_interface::write_interface(): "
            " start_request() hasn't been called");
    }
    return _srv->write_interface();
}

void server_request_interface::finish_request(
    const server::response_callback_t & callback)
{
    if(!_srv->_start_request_called)
        start_request();

    // requestor may have written additional data,
    //   and already waited for those writes to finish
    if(_srv->has_queued_writes())
    {
        _srv->write_queued(boost::bind(
            &server::on_request_written, _srv, _1, callback));
    }
    else
    {
        // write already completed; dispatch directly
        _srv->get_io_service()->dispatch(boost::bind(
            &server::on_request_written, _srv,
            boost::system::error_code(), callback));
    }
}

server_request_interface server_request_interface::null_instance()
{ return server_request_interface((server_ptr_t())); }

///////////////////////////////////////////////////////////////////////////////
//  server::response_interface

server_response_interface::server_response_interface(const server::ptr_t & p)
 : _srv(p)
{ }

const core::protobuf::SamoaResponse &
server_response_interface::get_message() const
{ return _srv->_response; }

core::stream_protocol::read_interface_t &
server_response_interface::read_interface()
{ return _srv->read_interface(); }

bool server_response_interface::is_error()
{
    const core::protobuf::SamoaResponse & resp = get_message();
    return resp.type() == core::protobuf::ERROR;
}

void server_response_interface::finish_response()
{
    if(!_srv->_response.closing())
    {
        // post next response read
        _srv->get_io_service()->post(boost::bind(
            &server::on_next_response, _srv));
    }
    else
    {
        // server indicated it's closing its
        //   connection: close ours too
        _srv->close();
    }
}

///////////////////////////////////////////////////////////////////////////////
//  server interface

server::~server()
{
    LOG_DBG("destroyed");    
}

void server::schedule_request(const server::request_callback_t & callback)
{
    get_io_service()->dispatch(boost::bind(
        &server::on_schedule_request, shared_from_this(), callback));
}

void server::close()
{
    _timeout_timer.cancel();
    core::stream_protocol::close();
}

void server::on_schedule_request(const server::request_callback_t & callback)
{
    if(_error)
    {
        // a connection error is already set; return failure
        request_interface null_int((ptr_t()));
        callback(_error, null_int);
        return;
    }

    if(_in_request)
    {
        // we're in the process of writing a request already
        _request_queue.push_back(callback);
        _queue_size += 1;
    }
    else
    {
        _in_request = true;

        // no requests are currently being written-- start one
        get_io_service()->post(boost::bind(callback,
            boost::system::error_code(),
            request_interface(shared_from_this())));
    }
}

void server::on_request_written(const boost::system::error_code & ec,
    const server::response_callback_t & callback)
{
    if(_error)
    {
        // a connection error is already set; return failure
        response_interface null_int((ptr_t()));
        callback(_error, null_int);
        return;
    }

    _response_queue.push_back(callback);

    if(ec)
    {
        on_error(ec);
        return;
    }

    // there was no previous pending response; start a new timeout period
    if(_response_queue.size() == 1 && !_ignore_timeout)
    {
        _timeout_timer.expires_from_now(
            boost::posix_time::milliseconds(_timeout_ms));
        _timeout_timer.async_wait(boost::bind(
            &server::on_timeout, shared_from_this(), _1));
    }

    _start_request_called = false;

    // start another request, if any are pending
    if(!_request_queue.empty())
    {
        request_callback_t callback = _request_queue.front();
        _request_queue.pop_front();

        get_io_service()->post(boost::bind(callback,
            boost::system::error_code(),
            request_interface(shared_from_this())));
    }
    else
        _in_request = false;
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
        on_error(ec);
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
        on_error(ec);
        return;
    }

    _proto_in_adapter.reset(read_body);
    if(!_response.ParseFromZeroCopyStream(&_proto_in_adapter))
    {
        // protobuf parse failure
        on_error(boost::system::errc::make_error_code(
            boost::system::errc::bad_message));
        return;
    }

    assert(!_response_queue.empty());

    // invoke callback
    response_interface resp_int(shared_from_this());
    _response_queue.front()(boost::system::error_code(), resp_int);

    // remove callback from queue
    _response_queue.pop_front();
    _queue_size -= 1;

    // ignore a timeout, as we've recieved a
    //   response during the timeout period
    _ignore_timeout = true;
}

void server::on_timeout(const boost::system::error_code & ec)
{
    if(ec == boost::asio::error::operation_aborted)
    {
        // timeout timer was cancelled; not an error
        return;
    }
    assert(!ec);

    if(_ignore_timeout)
    {
        // _timeout_timer expired, but we've recieved
        //   responses during the timeout period

        if(!_response_queue.empty())
        {
            // if there are remaining queued respones,
            //   start a new timeout interval

            _timeout_timer.expires_from_now(
                boost::posix_time::milliseconds(_timeout_ms));
            _timeout_timer.async_wait(boost::bind(
                &server::on_timeout, shared_from_this(), _1));

            _ignore_timeout = false; 
        }
    }
    else if(!ec && !_ignore_timeout)
    {
        // treat this timeout as an error
        on_error(boost::system::errc::make_error_code(
            boost::system::errc::timed_out));
    }
}

void server::on_error(const boost::system::error_code & ec)
{
    _error = ec;
    response_interface null_resp_int((ptr_t()));
    request_interface  null_req_int((ptr_t()));

    // post errors to all pending callbacks
    while(!_response_queue.empty())
    {
        get_io_service()->post(boost::bind(
            _response_queue.front(), _error, null_resp_int));
        _response_queue.pop_front();
    }
    while(!_request_queue.empty())
    {
        get_io_service()->post(boost::bind(
            _request_queue.front(), _error, null_req_int));
        _request_queue.pop_front();
    }

    close(); 
}

}
}

