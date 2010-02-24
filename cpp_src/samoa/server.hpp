#ifndef SAMOA_SERVER_HPP
#define SAMOA_SERVER_HPP

#include "samoa/fwd.hpp"
#include "common/reactor.hpp"
#include <boost/enable_shared_from_this.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/asio.hpp>
#include <string>

#include "common/rolling_hash.hpp"
#include "common/buffer_region.hpp"

namespace samoa {

class server :
    public  boost::enable_shared_from_this<server>,
    private boost::noncopyable
{
public:
    
    typedef boost::shared_ptr<server> ptr_t;
    
    server(
        const std::string & host,
        const std::string & port,
        size_t max_backlog_size,
        const common::reactor::ptr_t &
    );
    
    ~server();
    
    void shutdown();
    
    common::rolling_hash<> _rhash;
    common::buffer_ring _bring;
    
private:
    
    void on_shutdown();
    
    // Accept handler
    void on_accept(const boost::system::error_code & e);
    
    // Named server endpoint
    const std::string _host, _port;
    
    // Reactor w/ underlying io_service of the server
    common::reactor::ptr_t _reactor;
    
    // Accepting socket
    std::auto_ptr<boost::asio::ip::tcp::acceptor> _accept_sock;
    
    // Next connection to accept
    std::auto_ptr<boost::asio::ip::tcp::socket> _next_sock;
};

} // end namespace samoa 

#endif // guard

