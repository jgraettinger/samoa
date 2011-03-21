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

    // Returns an unusable (semantically null) instance
    static server_request_interface null_instance();

private:

    // only server may construct, though anybody may copy
    friend class server;
    explicit server_request_interface(const server_ptr_t & p);

    server_ptr_t _srv;
};

class server_response_interface
{
public:

    server_response_interface();

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

    virtual ~server();

    // Argument callback is called when a request is ready to be written
    //  to the server, and is passed a request_interface instance
    void schedule_request(const request_callback_t &);

    unsigned get_timeout_ms()
    { return _timeout_ms; }

    void set_timeout_ms(unsigned timeout_ms)
    { _timeout_ms = timeout_ms; }

    unsigned get_queue_size()
    { return _queue_size; }

    unsigned get_latency_ms()
    { return 1; }

    void close();

private:

    friend class server_request_interface;
    friend class server_response_interface;

    static void on_connect(const boost::system::error_code &,
        const core::proactor::ptr_t &,
        std::unique_ptr<boost::asio::ip::tcp::socket> &,
        const connect_to_callback_t &);

    server(const core::proactor::ptr_t &,
        std::unique_ptr<boost::asio::ip::tcp::socket> &);

    void on_schedule_request(const request_callback_t &);

    void on_request_written(const boost::system::error_code &,
        const response_callback_t &);

    void on_response_length(
        const boost::system::error_code &, const core::buffer_regions_t &);

    // reads server response & dispatches through strand to on_response_pop()
    void on_response_body(
        const boost::system::error_code &, const core::buffer_regions_t &);

    void on_timeout(const boost::system::error_code &);
    void on_error(const boost::system::error_code &);

    boost::system::error_code _error;

    bool _in_request;
    bool _start_request_called;

    core::protobuf::SamoaRequest   _request;
    core::protobuf::SamoaResponse _response;

    core::zero_copy_output_adapter _proto_out_adapter;
    core::zero_copy_input_adapter  _proto_in_adapter;

    std::list<request_callback_t> _request_queue;
    std::list<response_callback_t> _response_queue;
    unsigned _queue_size;

    bool _ignore_timeout;
    unsigned _timeout_ms;
    boost::asio::deadline_timer _timeout_timer;

    friend class server_private_ctor;
};

}
}

#endif

