
#include "samoa/server/listener.hpp"
#include "samoa/server/context.hpp"
#include "samoa/server/protocol.hpp"
#include "samoa/server/client.hpp"
#include "samoa/core/proactor.hpp"
#include "samoa/log.hpp"
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>

namespace samoa {
namespace server {

using namespace boost::asio;

listener::listener(const context_ptr_t & context, const protocol_ptr_t & protocol)
 : _context(context),
   _protocol(protocol),
   _proactor(core::proactor::get_proactor())
{
    core::io_service_ptr_t io_srv = _proactor->serial_io_service();

    // build resolution query
    ip::tcp::resolver::query query(context->get_server_hostname(),
        boost::lexical_cast<std::string>(context->get_server_port()));

    // blocks, & throws on resolution failure
    ip::tcp::endpoint ep = *ip::tcp::resolver(*io_srv).resolve(query);

    // create & listen on accepting socket, reusing port
    _accept_sock.reset(new ip::tcp::acceptor(*io_srv, ep));

    // TODO(johng) make this a configurable?
    _accept_sock->listen(5);

    // next connection to accept
    on_accept(boost::system::error_code());

    LOG_INFO("");
}

listener::~listener()
{
    LOG_INFO("");
}

void listener::cancel()
{
    LOG_INFO("");

    _accept_sock->get_io_service().dispatch(
        boost::bind(&listener::on_cancel, shared_from_this()));
}

std::string listener::get_address()
{ return _accept_sock->local_endpoint().address().to_string(); }

unsigned short listener::get_port()
{ return _accept_sock->local_endpoint().port(); }

void listener::on_accept(const boost::system::error_code & ec)
{
    if(ec == boost::system::errc::operation_canceled)
    {
        LOG_INFO("accept cancelled");
        return;
    }
    if(ec)
    {
        LOG_ERR(ec.message());
        return;
    }

    LOG_INFO("");

    if(_next_sock.get())
    {
        // Create a client to service the socket
        // Lifetime is managed by client's use in callbacks. Eg, it's
        //  auto-destroyed when it falls out of the event-loop
        client::ptr_t c(boost::make_shared<client>(
            _context, _protocol, _next_io_srv, _next_sock));

        c->init();
    }

    // Next connection to accept
    _next_io_srv = _proactor->serial_io_service();
    _next_sock.reset(new ip::tcp::socket(*_next_io_srv));

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
    LOG_INFO("");
    _accept_sock.reset();
    _next_sock.reset();
}

}
}

