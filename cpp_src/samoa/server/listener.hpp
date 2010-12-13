#ifndef SAMOA_SERVER_LISTENER_HPP
#define SAMOA_SERVER_LISTENER_HPP

#include "samoa/server/fwd.hpp"
#include <boost/asio.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/shared_ptr.hpp>
#include <string>

namespace samoa {
namespace server {

class listener :
    public boost::enable_shared_from_this<listener>,
    private boost::noncopyable
{
public:

    typedef boost::shared_ptr<listener> ptr_t;

    listener(std::string host, std::string port, unsigned listen_backlog,
        context_ptr_t, protocol_ptr_t);

    ~listener();

    void cancel();

private:

    void on_accept(const boost::system::error_code & ec);
    void on_cancel();

    const std::string _host, _port;

    context_ptr_t  _context;
    protocol_ptr_t _protocol;

    // Accepting socket
    std::auto_ptr<boost::asio::ip::tcp::acceptor> _accept_sock;
    
    // Next connection to accept
    std::auto_ptr<boost::asio::ip::tcp::socket> _next_sock;
};

}
}

#endif

