
#include "samoa/client/server.hpp"
#include <boost/smart_ptr/make_shared.hpp>
#include <iostream>

namespace samoa {
namespace client {

using namespace boost::asio;
using namespace std;

// default timeout of 1 minute
unsigned default_timeout_ms = 60 * 1000;

///////////////////////////////////////////////////////////////////////////////
// Connection & construction 

// private, friend constructor-class for use with boost::make_shared
class server_priv : public server
{
public:

    server_priv(const core::proactor::ptr_t & proactor,
        std::unique_ptr<ip::tcp::socket> & sock)
     : server(proactor, sock)
    { }
};

/* static */ core::connection_factory::ptr_t server::connect_to(
    const core::proactor::ptr_t & proactor,
    const std::string & host,
    const std::string & port,
    const server::connect_to_callback_t & callback)
{
    return core::connection_factory::connect_to(
        proactor, host, port,
        boost::bind(&server::on_connect, _1, proactor, _2, callback));
}

/* static */ void server::on_connect(
    const boost::system::error_code & ec,
    const core::proactor::ptr_t & proactor,
    std::unique_ptr<ip::tcp::socket> & sock,
    const server::connect_to_callback_t & callback)
{
    if(ec)
    { callback(ec, ptr_t()); }
    else
    { 
        ptr_t p(boost::make_shared<server_priv>(proactor, sock));

        // start initial response read
        p->read_data(2, boost::bind(
            &server::on_response_length, p, _1, _3));

        callback(ec, p);
    }
}

server::server(const core::proactor::ptr_t & proactor,
    std::unique_ptr<boost::asio::ip::tcp::socket> & sock)
 :  core::stream_protocol(sock),
    _proactor(proactor),
    _strand(get_socket().get_io_service()),
    _timeout_ms(default_timeout_ms),
    _timeout_timer(get_socket().get_io_service())
{ }

///////////////////////////////////////////////////////////////////////////////
//  server::request_interface

server_request_interface::server_request_interface(const server::ptr_t & p)
 : _srv(p)
{
    if(_srv)
    {
        assert(!_srv->has_queued_writes());
        _srv->_start_called = false;
    }
}

core::protobuf::SamoaRequest & server_request_interface::get_request()
{ return _srv->_request; }

void server_request_interface::start_request()
{
    if(_srv->_start_called)
        return;

    _srv->_start_called = true;

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

    // write serialized output regions
    _srv->queue_write(_srv->_proto_out_adapter.output_regions());

    // clear state for next operation
    _srv->_proto_out_adapter.reset();
}

core::stream_protocol::write_interface_t &
    server_request_interface::write_interface()
{
    if(!_srv->_start_called)
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
    // synchronized call to queue_response
    _srv->_strand.dispatch(boost::bind(
        &server::queue_response, _srv, callback));

    start_request();

    // requestor may have written additional data,
    //   and already waited for those writes to finish
    if(_srv->has_queued_writes())
    {
        // write remaining data / synchronized call to deque_request
        _srv->write_queued(_srv->_strand.wrap(
            boost::bind(&server::deque_request, _srv, _1)));
    }
    else
    {
        // synchronized call to deque_request 
        _srv->_strand.dispatch(boost::bind(&server::deque_request,
            _srv, boost::system::error_code()));
    }
}

///////////////////////////////////////////////////////////////////////////////
//  server::response_interface

server_response_interface::server_response_interface(const server::ptr_t & p)
 : _srv(p)
{ }

const core::protobuf::SamoaResponse & server_response_interface::get_response() const
{ return _srv->_response; }

core::stream_protocol::read_interface_t &
    server_response_interface::read_interface()
{ return _srv->read_interface(); }

void server_response_interface::finish_response()
{
    // start next response read
    _srv->read_data(2, boost::bind(
        &server::on_response_length, _srv, _1, _3));
}

///////////////////////////////////////////////////////////////////////////////
//  server

void server::schedule_request(const server::request_callback_t & callback)
{
    _strand.dispatch(boost::bind(&server::queue_request,
        shared_from_this(), callback));
}

void server::on_response_length(const boost::system::error_code & ec,
    const core::buffer_regions_t & read_body)
{
    if(ec)
    {
        // synchronized call to errored
        _strand.dispatch(boost::bind(
            &server::errored, shared_from_this(), ec));
        return;
    }

    uint16_t len;
    std::copy(
        boost::asio::buffers_begin(read_body),
        boost::asio::buffers_end(read_body),
        (char*) &len);

    read_data(ntohs(len), boost::bind(&server::on_response_body,
        shared_from_this(), _1, _3));
}

void server::on_response_body(const boost::system::error_code & ec,
    const core::buffer_regions_t & read_body)
{
    if(ec)
    {
        // synchronized call to errored
        _strand.dispatch(boost::bind(
            &server::errored, shared_from_this(), ec));
        return;
    }

    _proto_in_adapter.reset(read_body);
    if(!_response.ParseFromZeroCopyStream(&_proto_in_adapter))
    {
        // synchronized call to errored
        _strand.dispatch(boost::bind(
            &server::errored, shared_from_this(), 
            boost::system::errc::make_error_code(
                boost::system::errc::bad_message)));
        return;
    }

    // synchronized call to deque_response
    _strand.dispatch(boost::bind(
        &server::deque_response, shared_from_this()));
}

///////////////////////////////////////////////////
// SYNCHRONIZED by _strand

void server::queue_request(const server::request_callback_t & callback)
{
    if(_error)
    {
        // a connection error is set; post failure
        request_interface null_int((ptr_t()));
        _proactor->get_nonblocking_io_service().post(
            boost::bind(callback, _error, null_int));
        return;
    }

    _request_queue.push_back(callback);

    // If this is the only element, there is no current
    //   request-write operation.
    if(_request_queue.size() == 1)
    {
        // post request callback
        request_interface req_int(shared_from_this());
        _proactor->get_nonblocking_io_service().post(boost::bind(
            callback, boost::system::error_code(), req_int));
    }
}

void server::deque_request(const boost::system::error_code & ec)
{
    if(ec)
    {
        errored(ec);
        return;
    }

    // front callback has completed
    _request_queue.pop_front();

    if(!_request_queue.empty())
    {
        // post the next request callback
        request_interface req_int(shared_from_this());
        _proactor->get_nonblocking_io_service().post(boost::bind(
            _request_queue.front(), boost::system::error_code(), req_int));
    }
}

void server::queue_response(const server::response_callback_t & callback)
{
    if(_error)
    {
        // a connection error is set; post failure
        response_interface null_int((ptr_t()));
        _proactor->get_nonblocking_io_service().post(
            boost::bind(callback, _error, null_int));
        return;
    }

    if(_response_queue.empty())
    {
        // start a new timeout period
        _timeout_timer.expires_from_now(
            boost::posix_time::milliseconds(_timeout_ms));
        _timeout_timer.async_wait(_strand.wrap(boost::bind(
            &server::errored, shared_from_this(), _1)));

        _ignore_timeout = false; 
    }

    _response_queue.push_back(callback);
}

void server::deque_response()
{
    assert(!_response_queue.empty());

    // invoke callback
    response_interface resp_int(shared_from_this());
    _proactor->get_nonblocking_io_service().post(boost::bind(
        _response_queue.front(), boost::system::error_code(), resp_int));

    // remove callback from queue
    _response_queue.pop_front();

    // ignore a timeout, as we've recieved a
    //   response during the timeout period
    _ignore_timeout = true;
}

void server::errored(const boost::system::error_code & ec)
{
    std::cerr << ec.value() << ec << std::endl;

    if(false && _ignore_timeout)
    {
        // _timeout_timer expired, but we've recieved
        //   responses during the timeout period

        if(!_response_queue.empty())
        {
            // if there are remaining queued respones,
            //   start a new timeout interval

            _timeout_timer.expires_from_now(
                boost::posix_time::milliseconds(_timeout_ms));
            _timeout_timer.async_wait(_strand.wrap(boost::bind(
                &server::errored, shared_from_this(), _1)));

            _ignore_timeout = false; 
        }
        return;
    }

    _error = ec;
    response_interface null_resp_int((ptr_t()));
    request_interface  null_req_int((ptr_t()));

    while(!_response_queue.empty())
    {
        _proactor->get_nonblocking_io_service().post(boost::bind(
            _response_queue.front(), _error, null_resp_int));
        _response_queue.pop_front();
    }
    while(!_request_queue.empty())
    {
        _proactor->get_nonblocking_io_service().post(boost::bind(
            _request_queue.front(), _error, null_req_int));
        _request_queue.pop_front();
    }
}

}
}

