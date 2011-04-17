
#include "samoa/server/listener.hpp"
#include "samoa/server/context.hpp"
#include "samoa/server/protocol.hpp"
#include "samoa/server/client.hpp"
#include "samoa/core/proactor.hpp"
#include <boost/asio.hpp>

#include <iostream>

namespace samoa {
namespace server {

using namespace boost::asio;

listener::listener(std::string host, std::string port, unsigned listen_backlog,
    context::ptr_t ctxt, protocol::ptr_t prot)
 : _context(ctxt), _protocol(prot)
{
    core::proactor::ptr_t proactor = _context->get_proactor();

    ip::tcp::resolver resolver(*(proactor->serial_io_service()));

    // Blocks, & throws on resolution failure
    ip::tcp::endpoint ep = *resolver.resolve(
        ip::tcp::resolver::query(host, port));

    // Create & listen on accepting socket, reusing port
    _accept_sock.reset(new ip::tcp::acceptor(
        *(proactor->serial_io_service()), ep));

    _accept_sock->listen(listen_backlog);

    // Next connection to accept
    on_accept(boost::system::error_code());

    std::cerr << "listener::listener()" << std::endl;
}

listener::~listener()
{
    std::cerr << "listener::~listener()" << std::endl;
}

void listener::cancel()
{
    std::cerr << "listener::cancel()" << std::endl;

    _accept_sock->get_io_service().dispatch(
        boost::bind(&listener::on_cancel, shared_from_this()));
}

std::string listener::get_address()
{ return _accept_sock->local_endpoint().address().to_string(); }

unsigned listener::get_port()
{ return _accept_sock->local_endpoint().port(); }

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

    std::cerr << "listener::on_accept() begin" << std::endl;

    if(_next_sock.get())
    {
        // Create a client to service the socket
        // Lifetime is managed by client's use in callbacks. Eg, it's
        //  auto-destroyed when it falls out of event handler state
        client::ptr_t c(boost::make_shared<client>(
            _context, _protocol, _next_io_srv, _next_sock));

        c->init();
    }

    // Next connection to accept
    _next_io_srv = _context->get_proactor()->serial_io_service();
    _next_sock.reset(new ip::tcp::socket(*_next_io_srv));

    // Schedule call on accept. Note that bound handler
    // DOES NOT have a smart-pointer. This means, when
    // the owning pointer goes out of scope, and no
    // active connections remain, the server will exit.
    _accept_sock->async_accept(*_next_sock, boost::bind(
            &listener::on_accept, this,
            boost::asio::placeholders::error));

    std::cerr << "listener::on_accept() end" << std::endl;
    return;
}

void listener::on_cancel()
{
    std::cerr << "listener::on_cancel()" << std::endl;
    _accept_sock.reset();
    _next_sock.reset();
}

}
}

