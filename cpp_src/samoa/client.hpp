#ifndef SAMOA_CLIENT_HPP
#define SAMOA_CLIENT_HPP

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

class client :
    public boost::enable_shared_from_this<client>,
    protected stream_protocol
{
public:
    
    typedef boost::shared_ptr<client> ptr_t;
    
    client(
        const server_ptr_t &,
        std::auto_ptr<boost::asio::ip::tcp::socket>
    );
    
    // Initiates socket operations for this client;
    //  could be folded into ctor, except shared_from_this()
    //  requires external owning shared_ptr
    void init();
    
    // writes the response context in the client's
    //  request object back to the client
    void finish_response();
    
private:
    
    // individual command handlers
    void on_ping( const stream_protocol::match_results_t &);
    void on_shutdown( const stream_protocol::match_results_t &);
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
    
    // write completion handler
    //  listening for the next request does not begin until
    //  writeback of the previous request complets.
    void on_write( const boost::system::error_code & ec);
    
    // strong pointer to server
    server_ptr_t _server;
    
    // reusable request container
    request::ptr_t _request;
    
    bool _close_pending;
    
    static void build_command_table();
};

} // end namespace samoa 

#endif //guard

