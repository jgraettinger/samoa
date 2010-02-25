
#include "samoa/server.hpp"
#include "samoa/client.hpp"
#include "common/ref_buffer.hpp"
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <iostream>

namespace samoa {

using boost::asio::ip::tcp;
using namespace boost;

server::server(
    const std::string & host,
    const std::string & port,
    size_t max_backlog_size,
    const common::reactor::ptr_t & reactor
) :
    _host(host),
    _port(port),
    _reactor(reactor)
{
    tcp::resolver resolver(_reactor->io_service());
    
    // Blocks, & throws on resolution failure
    tcp::endpoint ep = *resolver.resolve(
        tcp::resolver::query(host, port)
    );
    
    // Create & listen on accepting socket
    _accept_sock = std::auto_ptr<tcp::acceptor>(
        new tcp::acceptor(
            _reactor->io_service(),
            ep,
            true // reuse address
        )
    );
    _accept_sock->listen(max_backlog_size);
    
    // Next connection to accept
    _next_sock.reset( new tcp::socket(_reactor->io_service()));
    
    // Schedule call on accept. Note that bound handler
    // DOES NOT have a smart-pointer. This means, when
    // the owning pointer goes out of scope, and no
    // active connections remain, the server will exit.
    _accept_sock->async_accept(
        *_next_sock,
        boost::bind(
            &server::on_accept,
            this,
            asio::placeholders::error
        )
    );

    std::cerr << "server created" << std::endl;
}

server::~server(){
    on_shutdown();
    std::cerr << "server destroyed" << std::endl;
}

void server::shutdown()
{
    std::cerr << "shutdown" << std::endl;
    // Ask io_service to run the handler soon,
    // but not within the current stack frame
    _reactor->call_later(
        boost::bind(
            &server::on_shutdown,
            shared_from_this()
        )
    );
}

void server::on_shutdown()
{
    std::cerr << "on_shutdown" << std::endl;
    _accept_sock.reset();
    _next_sock.reset();
}

void server::on_accept(const boost::system::error_code & ec)
{
    if(ec == boost::system::errc::operation_canceled)
    {
        std::cerr << "accept cancelled" << std::endl;
        return;
    }
    
    if(ec)
    {
        std::cerr << "read error: " << ec.value() << " " << ec.message();
        return;
    }
    
    // Create a client to service the socket
    // Lifetime is managed by client's use in callbacks. Eg, it's
    //  auto-destroyed when it falls out of event handler state
    client::new_client(shared_from_this(), _next_sock);
    
    // Next connection to accept
    _next_sock.reset( new tcp::socket(_reactor->io_service()));
    
    // Schedule call on accept. Note that bound handler
    // DOES NOT have a smart-pointer. This means, when
    // the owning pointer goes out of scope, and no
    // active connections remain, the server will exit.
    _accept_sock->async_accept(
        *_next_sock,
        boost::bind(
            &server::on_accept,
            this,
            asio::placeholders::error
        )
    );
    return;
}

} // end namespace samoa 

