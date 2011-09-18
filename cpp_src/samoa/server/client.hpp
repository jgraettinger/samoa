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

    // Expose the read/write interfaces of the underlying stream_protocol
    using core::stream_protocol::read_interface;
    using core::stream_protocol::write_interface;
    using core::stream_protocol::has_queued_writes;

    void finish_response(bool close);

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

    bool _ignore_timeout;
    unsigned _timeout_ms;
    boost::asio::deadline_timer _timeout_timer;

    void on_next_request();

    void on_request_length(const boost::system::error_code &,
        const core::buffer_regions_t &);

    void on_request_body(const boost::system::error_code &,
        const core::buffer_regions_t &);

    void on_request_data_block(const boost::system::error_code &,
        unsigned, const core::buffer_regions_t &, const request_state_ptr_t &);

    void on_response_finish(const boost::system::error_code &, bool);

    void on_timeout(boost::system::error_code);
};

}
}

#endif

