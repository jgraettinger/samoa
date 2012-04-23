
#include "samoa/server/listener.hpp"
#include "samoa/server/context.hpp"
#include "samoa/server/protocol.hpp"
#include "samoa/server/client.hpp"
#include "samoa/core/proactor.hpp"
#include "samoa/log.hpp"
#include <boost/asio.hpp>
#include <boost/lexical_cast.hpp>
#include <functional>

namespace samoa {
namespace server {

using namespace boost::asio;

listener::listener(const context_ptr_t & context,
    const protocol_ptr_t & protocol)
 : _context(context),
   _protocol(protocol),
   _accept_sock(*core::proactor::get_proactor()->serial_io_service())
{
    std::string str_port = boost::lexical_cast<std::string>(
        context->get_server_port());

    // build resolution query
    ip::tcp::resolver::query query(context->get_server_hostname(), str_port);

    // blocks, & throws on resolution failure
    ip::tcp::endpoint ep = *ip::tcp::resolver(
        _accept_sock.get_io_service()).resolve(query);

    // open & bind the listening socket
    _accept_sock.open(ep.protocol());
    _accept_sock.set_option(ip::tcp::acceptor::reuse_address(true));
    _accept_sock.bind(ep);
    _accept_sock.listen();

    LOG_DBG("");
}

listener::~listener()
{
    _context->drop_listener(reinterpret_cast<size_t>(this));
    LOG_DBG("");
}

std::string listener::get_address()
{ return _accept_sock.local_endpoint().address().to_string(); }

unsigned short listener::get_port()
{ return _accept_sock.local_endpoint().port(); }

void listener::initialize()
{
    _context->add_listener(reinterpret_cast<size_t>(this),
        shared_from_this());

    // next connection to accept
    on_accept(boost::system::error_code());
}

void listener::shutdown()
{
    LOG_DBG("");

    // release resources in acceptor's io_service
    _accept_sock.get_io_service().dispatch(
        [self = shared_from_this()]()
        {
            self->_accept_sock.close();
            self->_next_sock.reset();
            self->_next_io_srv.reset();
        });
}

void listener::on_accept(const boost::system::error_code & ec)
{
    if(ec == boost::system::errc::operation_canceled)
    {
        LOG_DBG("accept cancelled");
        return;
    }
    if(ec)
    {
        LOG_ERR(ec.message());
        return;
    }

    LOG_DBG("");

    if(_next_sock.get())
    {
        // we internally buffer responses already when building them,
        //   and don't want additional Nagle delay
        _next_sock->set_option(ip::tcp::no_delay(true));

        // Create a client to service the socket
        client::ptr_t cl = boost::make_shared<client>(_context,
            _protocol, std::move(_next_sock), std::move(_next_io_srv));
        cl->initialize();

        // track strong-ptr to guard client lifetime
        _context->add_client(reinterpret_cast<size_t>(cl.get()), cl);
    }

    // Next connection to accept
    _next_io_srv = core::proactor::get_proactor()->serial_io_service();
    _next_sock.reset(new ip::tcp::socket(*_next_io_srv));

    // Schedule call on accept
    _accept_sock.async_accept(*_next_sock,
        std::bind(&listener::on_accept,
            shared_from_this(), std::placeholders::_1));

    return;
}

}
}

