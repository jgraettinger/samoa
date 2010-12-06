
#include "samoa/server/listener.hpp"
#include "samoa/server/context.hpp"
#include "samoa/server/protocol.hpp"
#include "samoa/server/client.hpp"
#include "samoa/core/proactor.hpp"
#include <boost/asio.hpp>
#include <iostream>

namespace server {

using namespace boost::asio;
using namespace boost;

listener::listener(std::string host, std::string port, unsigned listen_backlog,
    context::ptr_t ctxt, protocol::ptr_t prot)
 : _host(host), _port(port), _context(ctxt), _protocol(prot)
{
    core::proactor::ptr_t proactor = _context->get_proactor();

    ip::tcp::resolver resolver(proactor->get_nonblocking_io_service());

    // Blocks, & throws on resolution failure
    ip::tcp::endpoint ep = *resolver.resolve(
        ip::tcp::resolver::query(host, port));

    // Create & listen on accepting socket, reusing port
    _accept_sock = std::auto_ptr<ip::tcp::acceptor>(new ip::tcp::acceptor(
        proactor->get_nonblocking_io_service(), ep, true));

    _accept_sock->listen(listen_backlog);

    // Next connection to accept
    on_accept(boost::system::error_code());
}

void listener::cancel()
{
    _context->get_proactor()->get_nonblocking_io_service().post(
        boost::bind(&listener::on_cancel, shared_from_this()));
}

void listener::on_accept(const boost::system::error_code & ec)
{
    if(ec == boost::system::errc::operation_canceled)
    {
        std::cerr << "accept cancelled" << std::endl;
        return;
    }
    if(ec)
    {
        std::cerr << "listener::on_accept: " << ec.message() << std::endl;
        return;
    }

    if(_next_sock.get())
    {
        // Create a client to service the socket
        // Lifetime is managed by client's use in callbacks. Eg, it's
        //  auto-destroyed when it falls out of event handler state
        client::ptr_t c(new client(_context, _protocol, _next_sock));

        _protocol->start(c);
    }

    // Next connection to accept
    _next_sock.reset(new ip::tcp::socket(
        _context->get_proactor()->get_nonblocking_io_service()));

    // Schedule call on accept. Note that bound handler
    // DOES NOT have a smart-pointer. This means, when
    // the owning pointer goes out of scope, and no
    // active connections remain, the server will exit.
    _accept_sock->async_accept(*_next_sock, boost::bind(
            &listener::on_accept, this,
            boost::asio::placeholders::error));

    return;
}

void listener::on_cancel()
{
    std::cerr << "listener::on_cancel()" << std::endl;
    _accept_sock.reset();
    _next_sock.reset();
}

}

