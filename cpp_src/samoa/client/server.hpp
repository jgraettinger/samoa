#ifndef SAMOA_CLIENT_SERVER_HPP
#define SAMOA_CLIENT_SERVER_HPP

#include "samoa/core/fwd.hpp"
#include "samoa/client/fwd.hpp"
#include "samoa/core/stream_protocol.hpp"
#include "samoa/core/connection_factory.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include "samoa/core/proactor.hpp"
#include <boost/asio.hpp>
#include <unordered_map>
#include <functional>
#include <memory>
#include <list>

namespace samoa {
namespace client {

typedef std::function<
    void(boost::system::error_code, server_request_interface)
> server_request_callback_t;

typedef std::function<
    void(boost::system::error_code, server_response_interface)
> server_response_callback_t;

typedef std::function<
    void(boost::system::error_code, server_ptr_t)
> server_connect_to_callback_t;


class server_request_interface
{
public:

    /// Constructs an unusable (null) instance
    server_request_interface() = default;

    // Where possible prefer move-construction
    //  (Note: looked into deleting copy-constructors, but asio
    //    enforces that handlers be copy-constructable, though
    //    it tries to use only move semantics)
    server_request_interface(server_request_interface &&) = default;
    server_request_interface(const server_request_interface &) = default;

    /*!
     * \brief Request to be written to the server.
     * This object is mutable and exclusive to the current holder
     *  of the server::request_interface.
     */
    core::protobuf::SamoaRequest & get_message() const;

    /*!
     * \brief Adds the single buffer_region as a request datablock
     * SamoaRequest::data_block_length is appropriately updated.
     */
    void add_data_block(const core::buffer_region &) const;

    /*!
     * \brief Adds the buffer-regions as a request datablock
     * SamoaRequest::data_block_length is appropriately updated.
     */
    void add_data_block(const core::buffer_regions_t &) const;

    /*!
     * \brief Adds the (byte) iteration-range as a request datablock
     * SamoaRequest::data_block_length is appropriately updated.
     */
    template<typename Iterator>
    void add_data_block(const Iterator & beg, const Iterator & end) const;

    /*!
     * \brief Writes the complete request to server.
     * Ownership of the request interface is released.
     *
     * @param callback Callback to invoke when this request's
     *  response is recieved from the server
     */
    void flush_request(server_response_callback_t);

    /*! \brief Cancels the current request
     * Ownership of the request interface is released.
     */
    void abort_request();

private:

    // only server may construct
    friend class server;
    explicit server_request_interface(server_ptr_t);

    server_ptr_t _srv;
};

class server_response_interface
{
public:

    /// Constructs an unusable (null) instance
    server_response_interface() = default;

    // Where possible prefer move construction
    server_response_interface(server_response_interface &&) = default;
    server_response_interface(const server_response_interface &) = default;

    /*!
     * \brief Response received from server
     */
    const core::protobuf::SamoaResponse & get_message() const;

    /*!
     * \brief Returns response error code (or 0 if none is set)
     */
    unsigned get_error_code() const;

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
    explicit server_response_interface(server_ptr_t);

    server_ptr_t _srv;
};

class server :
    public core::stream_protocol,
    public std::enable_shared_from_this<server>
{
public:

    typedef server_ptr_t ptr_t;

    typedef server_request_interface request_interface;
    typedef server_response_interface response_interface;

    typedef server_request_callback_t request_callback_t;
    typedef server_response_callback_t response_callback_t;

    static void connect_to(
        server_connect_to_callback_t,
        const std::string & host,
        unsigned short port);

    server(std::unique_ptr<boost::asio::ip::tcp::socket>,
        core::io_service_ptr_t);

    virtual ~server();

    /*!
     * \brief Schedules writing of a request
     *
     * Callback argument will be invoked with an exclusively-held
     *  server::request_interface, which may be used to write the
     *  request to the server
     */
    void schedule_request(request_callback_t);

private:

    using stream_protocol::read_interface_t;
    using stream_protocol::write_interface_t;

    friend class server_request_interface;
    friend class server_response_interface;

    typedef std::function<void(boost::system::error_code)> wrapped_callback_t;

    typedef std::list<wrapped_callback_t
        > pending_requests_t;
    typedef std::unordered_map<unsigned, wrapped_callback_t
        > pending_responses_t;

    static void on_connect(boost::system::error_code,
        std::unique_ptr<boost::asio::ip::tcp::socket>,
        core::io_service_ptr_t,
        server_connect_to_callback_t);

    static void on_schedule_request(ptr_t, request_callback_t);

    static void on_flush_request(ptr_t, response_callback_t);

    static void on_request_finish(
        write_interface_t::ptr_t,
        boost::system::error_code);

    void on_next_response();

    static void on_response_length(
        read_interface_t::ptr_t self,
        boost::system::error_code,
        core::buffer_regions_t);

    static void on_response_body(
        read_interface_t::ptr_t self,
        boost::system::error_code,
        core::buffer_regions_t);

    /*
     * Reentrant: reads response data blocks.
     *
     * When last data-block is read, dispatches to the response's
     * corresponding callback.
     */
    static void on_response_data_block(
        read_interface_t::ptr_t self,
        boost::system::error_code,
        core::buffer_regions_t,
        unsigned);

    /*
     * Manages common cleanup-work for a recieved error,
     * including notifying pending requests & responses
     * of the socket's error
     */
    void on_connection_error(boost::system::error_code);

    // modified exclusively through request_interface
    core::protobuf::SamoaRequest _samoa_request;
    core::buffer_regions_t _request_data;
    unsigned _next_request_id /*= 1*/;

    // exposed as immutable through response_interface
    core::protobuf::SamoaResponse _samoa_response;
    std::vector<core::buffer_regions_t> _response_data_blocks;

    bool _ready_for_write /*= true*/;

    pending_requests_t _pending_requests;
    pending_responses_t _pending_responses;
};

}
}

#include "samoa/client/server.impl.hpp"

#endif

