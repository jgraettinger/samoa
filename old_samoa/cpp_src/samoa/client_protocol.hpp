#ifndef SAMOA_CLIENT_PROTOCOL_HPP
#define SAMOA_CLIENT_PROTOCOL_HPP

#include "samoa/fwd.hpp"
#include "samoa/request.hpp"
#include "common/stream_protocol.hpp"
#include "common/ref_buffer.hpp"
#include <boost/asio.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/shared_ptr.hpp>
#include <memory>

namespace samoa {

using namespace common;

class client_protocol :
    public boost::enable_shared_from_this<client_protocol>,
    public stream_protocol
{
public:
    
    typedef boost::shared_ptr<client_protocol> ptr_t;
    
    // Creates a new client_protocol object, & initiates a read operation
    static ptr_t new_client_protocol(
        const server_ptr_t &,
        std::auto_ptr<boost::asio::ip::tcp::socket>
    );
    
    // Writes remaining response content to client,
    //  and initiates read of the next request (if applicable)
    void finish_response();
    
    request & get_request()
    { return _request; }
    
private:
    
    client_protocol(
        const server_ptr_t &,
        std::auto_ptr<boost::asio::ip::tcp::socket>
    );
    
    // individual command handlers
    void on_ping( const stream_protocol::match_results_t &);
    void on_shutdown( const stream_protocol::match_results_t &);
    void on_iter_keys( const stream_protocol::match_results_t &);
    void on_get( const stream_protocol::match_results_t &);
    void on_set( const stream_protocol::match_results_t &);
    void on_malformed( const stream_protocol::match_results_t &);
    
    // generic handler for data block of command
    void on_data( const boost::system::error_code &, size_t);
    
    // generic command handler/dispatcher
    void on_command(
        const boost::system::error_code & ec,
        const stream_protocol::match_results_t &,
        size_t matched_re_ind
    );
    
    // common handler for write completion of current request
    //  depending on request flags, may start another read
    //  or close the connection
    void on_finish(const boost::system::error_code & ec);
    
    // strong pointer to server
    server_ptr_t _server;
    
    request _request;
    
    static void build_command_table();
};

} // end namespace samoa 

#endif //guard

