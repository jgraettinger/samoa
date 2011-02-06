#ifndef SAMOA_CLIENT_SERVER_HPP
#define SAMOA_CLIENT_SERVER_HPP

#include "samoa/client/fwd.hpp"
#include "samoa/core/protobuf_helpers.hpp"
#include "samoa/core/stream_protocol.hpp"
#include "samoa/core/connection_factory.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include "samoa/core/proactor.hpp"
#include <boost/asio.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/function.hpp>
#include <memory>

namespace samoa {
namespace client {

typedef boost::function<
void(const boost::system::error_code &, server_request_interface)
> server_request_callback_t;

typedef boost::function<
void(const boost::system::error_code &, server_response_interface)
> server_response_callback_t;

class server_request_interface
{
public:

    // Request to be written to the server. This object is fully mutable
    //  and exclusively available to the current "renter" of the
    //  server::request_interface, *until start_request() is called*
    core::protobuf::SamoaRequest & get_request();

    // Serializes & writes the current core::protobuf::SamoaRequest
    //   Postcondition: the core::protobuf::SamoaRequest is no longer mutable
    void start_request(); 

    // Exposes the writing interface of the underlying stream_protocol
    //   Precondition: start_request() must have been called
    //    (an exception is thrown otherwise)
    core::stream_protocol::write_interface_t & write_interface();

    // Flushes remaining writes to the server.
    //   If start_request() hasn't yet been called, it will be.
    // Argument callback is invoked when the corresponding
    //   response core::protobuf::SamoaResponse has been recieved
    void finish_request(const server_response_callback_t &);

private:

    // only server may construct, though anybody may copy
    friend class server;
    explicit server_request_interface(const server_ptr_t & p);

    server_ptr_t _srv;
};

class server_response_interface
{
public:

    const core::protobuf::SamoaResponse & get_response() const;

    core::stream_protocol::read_interface_t & read_interface();

    // Called when reading of the response is complete.
    //  Releases ownership of the server::response_interface
    void finish_response();

private:

    // only server may construct
    friend class server;
    explicit server_response_interface(const server_ptr_t & p);

    server_ptr_t _srv;
};

class server :
    public boost::enable_shared_from_this<server>,
    public core::stream_protocol
{
public:

    typedef boost::shared_ptr<server> ptr_t;

    typedef server_request_interface request_interface;
    typedef server_response_interface response_interface;

    typedef server_request_callback_t request_callback_t;
    typedef server_response_callback_t response_callback_t;

    typedef boost::function<
        void(const boost::system::error_code &, server::ptr_t)
    > connect_to_callback_t;

    static core::connection_factory::ptr_t connect_to(
        const core::proactor::ptr_t &,
        const std::string & host,
        const std::string & port,
        const connect_to_callback_t &);

    // Argument callback is called when a request is ready to be written
    //  to the server, and is passed a request_interface instance
    void schedule_request(const request_callback_t &);

private:

    friend class server_request_interface;
    friend class server_response_interface;

    static void on_connect(const boost::system::error_code &,
        const core::proactor::ptr_t &,
        std::unique_ptr<boost::asio::ip::tcp::socket> &,
        const connect_to_callback_t &);

    server(const core::proactor::ptr_t &,
        std::unique_ptr<boost::asio::ip::tcp::socket> &);

    void on_response_length(
        const boost::system::error_code &, const core::buffer_regions_t &);

    // reads server response & dispatches through strand to on_response_pop()
    void on_response_body(
        const boost::system::error_code &, const core::buffer_regions_t &);

    ///////////////////////////////////////////////////
    // SYNCHRONIZED by _strand

    // add request to queue. if queue is empty,
    //   dispatch to pop_request
    void queue_request(const request_callback_t &);

    // deqeues a scheduled request handler, and calls back to it
    //  used as a write-completion handler for the previous request
    void deque_request(const boost::system::error_code &);

    // adds response callback to queue.
    void queue_response(const response_callback_t &);

    // called by on_response_body, once a
    //  protobuf response has already been parsed
    void deque_response();

    // sets a connection error, and calls-back to
    //   queued requests & responses with _error 
    void errored(const boost::system::error_code &);

    ///////////////////////////////////////////////////

    core::proactor::ptr_t _proactor;
    boost::asio::strand _strand;

    boost::system::error_code _error;

    bool _start_called;
    core::protobuf::SamoaRequest   _request;
    core::protobuf::SamoaResponse _response;

    core::zero_copy_output_adapter _proto_out_adapter;
    core::zero_copy_input_adapter  _proto_in_adapter;

    std::list<request_callback_t> _request_queue;
    std::list<response_callback_t> _response_queue;

    friend class server_priv;
};

}
}

#endif

