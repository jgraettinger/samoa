#ifndef SAMOA_SERVER_CLIENT_HPP
#define SAMOA_SERVER_CLIENT_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/core/fwd.hpp"
#include "samoa/core/protobuf_helpers.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include "samoa/core/stream_protocol.hpp"
#include "samoa/core/tasklet.hpp"
#include <boost/asio.hpp>

namespace samoa {
namespace server {

class client :
    public core::stream_protocol,
    public core::tasklet<client>
{
public:

    using core::tasklet<client>::ptr_t;

    client(context_ptr_t, protocol_ptr_t,
        core::io_service_ptr_t,
        std::unique_ptr<boost::asio::ip::tcp::socket> &);

    ~client();

    // both bases define equivalent methods; pick one
    using core::stream_protocol::get_io_service;

    const context_ptr_t & get_context() const
    { return _context; }

    const protocol_ptr_t & get_protocol() const
    { return _protocol; }

    // (Const) Request currently being processed on behalf of this client
    const core::protobuf::SamoaRequest & get_request() const
    { return _request; }

    // Response currently being written for return to client
    //   Note: the command handler invoked to managed this client request
    //   has sole rights to mutation of the response object until
    //   start_response() or finish_response() is called
    core::protobuf::SamoaResponse & get_response()
    { return _response; }

    const std::vector<core::buffer_regions_t> &
    get_request_data_blocks() const
    { return _request_data_blocks; }

    /*! \brief Helper for sending an error response to the client

    Precondition: start_response() cannot have been called yet
    send_error() does the following:
     - clears any set state in the SamoaResponse
     - sets the error type and field in the SamoaResponse,
        to the given code & value
     - calls finish_response()
    */
    void send_error(unsigned err_code, const std::string & err_msg,
        bool closing = false);

    void send_error(unsigned err_code, const boost::system::error_code &,
        bool closing = false);

    // Serializes & writes the current core::protobuf::SamoaResponse
    //   Postcondition: the core::protobuf::SamoaResponse is no longer mutable
    void start_response();

    // Expose the reading interface of the underlying stream_protocol
    using core::stream_protocol::read_interface;

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

    void run_tasklet();
    void halt_tasklet();

private:

    context_ptr_t _context;
    protocol_ptr_t _protocol;

    bool _start_called;

    core::protobuf::SamoaRequest   _request;
    core::protobuf::SamoaResponse _response;

    core::zero_copy_output_adapter _proto_out_adapter;
    core::zero_copy_input_adapter  _proto_in_adapter;

    std::vector<core::buffer_regions_t> _request_data_blocks;

    bool _ignore_timeout;
    unsigned _timeout_ms;
    boost::asio::deadline_timer _timeout_timer;

    void on_next_request();

    void on_request_length(
        const boost::system::error_code &, const core::buffer_regions_t &);

    void on_request_body(
        const boost::system::error_code &, const core::buffer_regions_t &);

    void on_request_data_block(
        const boost::system::error_code &, unsigned, const core::buffer_regions_t &);

    void on_response_finish(
        const boost::system::error_code &);

    void on_timeout(
        boost::system::error_code);
};

}
}

#endif

