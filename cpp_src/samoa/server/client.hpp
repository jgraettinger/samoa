#ifndef SAMOA_SERVER_CLIENT_HPP
#define SAMOA_SERVER_CLIENT_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/core/fwd.hpp"
#include "samoa/core/protobuf_helpers.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include "samoa/core/stream_protocol.hpp"
#include "samoa/core/tasklet.hpp"
#include "samoa/spinlock.hpp"
#include <boost/asio.hpp>
#include <list>

namespace samoa {
namespace server {

typedef boost::function<
    void(client_response_interface)
> client_response_callback_t;

/*!
 * Manages exclusive access to a client's underlying
 *  core::stream_protocol::write_interface.
 *
 * This facilitates delivery of out-of-order responses to the client:
 * as responses become available, handlers call schedule_response()
 * to arrange for eventual callback with an exclusive client_response_interface.
 *
 * When the response generation is complete, finish_response() is called to
 * flush remaining writes and notify the client that the next response handler
 * can be called back.
 */
class client_response_interface
{
public:

    /*!
     * Exposes the client's underlying write_interface_t
     * @return An exclusively-held write_interface
     */
    core::stream_protocol::write_interface_t & write_interface();

    /*!
     * Flushes remaining queued writes, and releases ownership
     * of the client_response_interface.
     */
    void finish_response();

private:

    friend class client;
    explicit client_response_interface(const client_ptr_t & c);

    client_ptr_t _client;
};


class client :
    public core::stream_protocol,
    public core::tasklet<client>
{
public:

    using core::tasklet<client>::ptr_t;

    typedef client_response_callback_t response_callback_t;
    typedef client_response_interface response_interface;

    static const unsigned max_request_concurrency;

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

    unsigned get_timeout_ms()
    { return _timeout_ms; }

    void set_timeout_ms(unsigned timeout_ms)
    { _timeout_ms = timeout_ms; }

    /*!
     * \brief Schedules writing of response.
     *
     * Callback argument will be invoked with an exclusively-held
     *  client::response_interface, which may be used to write the
     *  response to the client
     */
    void schedule_response(const response_callback_t &);

    void run_tasklet();
    void halt_tasklet();

private:

    friend class client_response_interface;

    /*
     * If a read-operation isn't already in progress and
     *  we haven't yet reach max request concurrency,
     *  begin a new request read (starting with length preamble)
     */
    void on_next_request();

    /*
     * Begins read of SamoaRequest body
     */
    void on_request_length(const boost::system::error_code &,
        const core::buffer_regions_t &);

    /*
     * Allocates a request_state; parses SamoaRequest; enters on_request_data_block
     */
    void on_request_body(const boost::system::error_code &,
        const core::buffer_regions_t &);

    /*
     * Re-entrant: reads request data blocks.
     *
     * When the last data-block is read, dispatches the request_state
     *  to the appropriate handler, and posts to on_next_request()
     */
    void on_request_data_block(const boost::system::error_code &,
        unsigned, const core::buffer_regions_t &,
        const request_state_ptr_t &);

    /*
     * Response scheduling workhorse.
     *
     * If we're ready to write a new response (_ready_for_write),
     *  and have a queued callback, that callback is posted.
     *
     * If a new callback is given, that callback is either
     *  posted (if we're ready to write, and there's no queued
     *  callback) or itself queued.
     *
     * @param is_write_complete Whether this call marks that a
     *  current response operation has completed.
     * @param new_callback A new response callback to invoke or queue.
     */
    void on_next_response(bool is_write_complete,
        const response_callback_t * new_callback);

    /*
     * Logs errors, and begins the next queued response.
     */
    void on_response_finish(const boost::system::error_code &);

    /*
     * If a client hasn't responded within the timeout period, and has
     *  no pending requests, then the client is stopped (socket is closed,
     *  timeout cancelled, etc).
     */
    void on_timeout(boost::system::error_code);

    const context_ptr_t _context;
    const protocol_ptr_t _protocol;

    bool _ready_for_write; // xthread

    std::list<response_callback_t> _queued_response_callbacks; // xthread

    unsigned _cur_requests_outstanding;

    bool _ignore_timeout;
    unsigned _timeout_ms;
    boost::asio::deadline_timer _timeout_timer;
    
    spinlock _lock;
};

}
}

#endif

