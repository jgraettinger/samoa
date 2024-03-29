#ifndef SAMOA_CLIENT_SERVER_HPP
#define SAMOA_CLIENT_SERVER_HPP

#include "samoa/core/fwd.hpp"
#include "samoa/client/fwd.hpp"
#include "samoa/core/stream_protocol.hpp"
#include "samoa/core/connection_factory.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include "samoa/core/proactor.hpp"
#include <boost/unordered_map.hpp>
#include <boost/asio.hpp>
#include <list>
#include <memory>

namespace samoa {
namespace client {

// would like these to pass a rvalue-reference, but this breaks
//  current boost::asio::io_service::wrap

typedef boost::function<
    void(const boost::system::error_code &, server_request_interface)
> server_request_callback_t;

typedef boost::function<
    void(const boost::system::error_code &, server_response_interface)
> server_response_callback_t;

typedef boost::function<
    void(const boost::system::error_code &, server_ptr_t)
> server_connect_to_callback_t;


class server_request_interface
{
public:

    /*!
     * \brief Request to be written to the server.
     * This object is mutable and exclusive to the current holder
     *  of the server::request_interface.
     */
    core::protobuf::SamoaRequest & get_message();

    /*! \brief Cancels the current request
     * Ownership of the request interface is released.
     */
    void abort_request();

    /*!
     * \brief Adds the const buffer-regions as a request datablock
     * SamoaRequest::data_block_length is appropriately updated.
     */
    void add_data_block(const core::const_buffer_regions_t &);

    /*!
     * \brief Adds the buffer-regions as a request datablock
     * SamoaRequest::data_block_length is appropriately updated.
     */
    void add_data_block(const core::buffer_regions_t &);

    /*!
     * \brief Adds the (byte) iteration-range as a request datablock
     * SamoaRequest::data_block_length is appropriately updated.
     */
    template<typename Iterator>
    void add_data_block(const Iterator & beg, const Iterator & end);

    /*!
     * \brief Writes the complete request to server.
     * Ownership of the request interface is released.
     *
     * @param callback Callback to invoke when this request's
     *  response is recieved from the server
     */
    void flush_request(const server_response_callback_t &);

    /*!
     *  \brief Returns an unusable (semantically null) instance
     */
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

    /*!
     * \brief Response received from server
     */
    const core::protobuf::SamoaResponse & get_message() const;

    /*!
     * \brief Returns response error code (or 0 if none is set)
     */
    unsigned get_error_code();

    /*!
     * \brief Response data blocks returned by the server
     */
    const std::vector<core::buffer_regions_t> &
        get_response_data_blocks() const;

    /*!
     * \brief Releases ownership of the server::response_interface
     *
     * Postcondition: the response interface is no longer used,
     *  and the server is free to begin the next response read.
     */
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

    typedef server_ptr_t ptr_t;

    typedef server_request_interface request_interface;
    typedef server_response_interface response_interface;

    typedef server_request_callback_t request_callback_t;
    typedef server_response_callback_t response_callback_t;

    static core::connection_factory::ptr_t connect_to(
        const server_connect_to_callback_t &,
        const std::string & host,
        unsigned short port);

    virtual ~server();

    /*!
     * \brief Schedules writing of a request
     *
     * Callback argument will be invoked with an exclusively-held
     *  server::request_interface, which may be used to write the
     *  request to the server
     */
    void schedule_request(const request_callback_t &);

private:

    friend class server_request_interface;
    friend class server_response_interface;

    typedef boost::unordered_map<unsigned, response_callback_t
        > pending_responses_t;

    static void on_connect(const boost::system::error_code &,
        const core::io_service_ptr_t &,
        std::unique_ptr<boost::asio::ip::tcp::socket> &,
        const server_connect_to_callback_t &);

    server(const core::io_service_ptr_t &,
        std::unique_ptr<boost::asio::ip::tcp::socket> &);

    /* 
     * Begins a new response read (starting with length preamble).
     */ 
    void on_next_response();

    // Begins read of SamoaResponse
    void on_response_length(const boost::system::error_code &,
        const core::buffer_regions_t &);

    // Parses SamoaResponse, and begins read of response data-blocks
    void on_response_body(const boost::system::error_code &,
        const core::buffer_regions_t &);

    /*
     * Reentrant: reads response data blocks.
     *
     * When last data-block is read, dispatches to the response's
     * corresponding callback.
     */
    void on_response_data_block(const boost::system::error_code &,
        unsigned, const core::buffer_regions_t &);

    /*
     * Manages common cleanup-work for a recieved error
     */
    void on_response_error(const boost::system::error_code &);

    /*
     * Request scheduling workhorse.
     *
     * If we're ready to write a new request (_ready_for_write),
     *  and have a queued callback, that callback is posted.
     *
     * If a new callback is given, that callback is either
     *  posted (if we're ready to write, and there's no queued
     *  callback) or itself queued.
     *
     * @param is_write_complete Whether this call marks that a
     *  current request operation has completed.
     * @param new_callback A new request callback to invoke or queue
     */
    void on_next_request(bool is_write_complete,
        const request_callback_t * new_callback);

    /*
     * Callback when a request has been written (or aborted)
     *
     * If the request-write failed, notifies the corresponding
     *  response callback.
     *
     * Begins the next queued request.
     */
    void on_request_finish(const boost::system::error_code &,
        unsigned request_id);

    // modified exclusively through request_interface
    core::protobuf::SamoaRequest _samoa_request;
    core::const_buffer_regions_t _request_data;
    unsigned _next_request_id;

    // exposed as immutable through response_interface
    core::protobuf::SamoaResponse _samoa_response;
    std::vector<core::buffer_regions_t> _response_data_blocks;

    bool _ready_for_write; // xthread

    std::list<request_callback_t> _queued_request_callbacks; // xthread
    pending_responses_t _pending_responses; // xthread

    spinlock _lock;

    friend class server_private_ctor;
};

}
}

#include "samoa/client/server.impl.hpp"

#endif

