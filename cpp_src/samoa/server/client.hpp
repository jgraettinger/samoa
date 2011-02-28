#ifndef SAMOA_SERVER_CLIENT_HPP
#define SAMOA_SERVER_CLIENT_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/core/protobuf_helpers.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include "samoa/core/stream_protocol.hpp"
#include <boost/asio.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/shared_ptr.hpp>
#include <memory>

namespace samoa {
namespace server {

class client :
    public core::stream_protocol,
    public boost::enable_shared_from_this<client>
{
public:

    typedef boost::shared_ptr<client> ptr_t;

    client(context_ptr_t, protocol_ptr_t,
        std::unique_ptr<boost::asio::ip::tcp::socket> &);

    // Begins reading requests
    void init();

    const context_ptr_t & get_context() const
    { return _context; }

    const protocol_ptr_t & get_protocol() const
    { return _protocol; }

    // (Const) Request currently being processed on behalf of this client
    const core::protobuf::SamoaRequest & get_request() const
    { return _request; }

    // Expose the reading interface of the underlying stream_protocol
    using core::stream_protocol::read_interface;

    // Response currently being written for return to client
    //   Note: the command handler invoked to managed this client request
    //   has sole rights to mutation of the response object until
    //   start_response() or finish_response() is called
    core::protobuf::SamoaResponse & get_response()
    { return _response; }

    // Helper which clears any set state in the response, and
    //  instead generates an error of the given type & value.
    // finish_response() must still be called to start the write
    void set_error(const std::string & err_type,
        const std::string & err_msg, bool closing);

    // Serializes & writes the current core::protobuf::SamoaResponse
    //   Postcondition: the core::protobuf::SamoaResponse is no longer mutable
    void start_response();

    // Exposes the writing interface of the underlying stream_protocol
    //   Precondition: start_response() must have been called
    //     (an exception is thrown otherwise)
    core::stream_protocol::write_interface_t & write_interface();

    // Flushes remaining data to the client (including the
    //   core::protobuf::SamoaResponse if start_response() has not been called)
    //   and starts the next request (if the last response didn't indicate
    //   the connection was closing)
    void finish_response();

    unsigned get_timeout_ms()
    { return _timeout_ms; }

    void set_timeout_ms(unsigned timeout_ms)
    { _timeout_ms = timeout_ms; }

    void close();

private:

    context_ptr_t _context;
    protocol_ptr_t _protocol;

    bool _start_called;

    core::protobuf::SamoaRequest   _request;
    core::protobuf::SamoaResponse _response;

    core::zero_copy_output_adapter _proto_out_adapter;
    core::zero_copy_input_adapter  _proto_in_adapter;

    bool _ignore_timeout;
    unsigned _timeout_ms;
    boost::asio::deadline_timer _timeout_timer;

    void on_request_length(
        const boost::system::error_code &, const core::buffer_regions_t &);

    void on_request_body(
        const boost::system::error_code &, const core::buffer_regions_t &);

    void on_response_finish(
        const boost::system::error_code &);

    void on_timeout(
        boost::system::error_code);
};

}
}

#endif

