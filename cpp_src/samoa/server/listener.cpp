
#include "samoa/server/listener.hpp"
#include "samoa/server/context.hpp"
#include "samoa/server/protocol.hpp"
#include "samoa/server/client.hpp"
#include "samoa/core/proactor.hpp"
#include "samoa/core/tasklet_group.hpp"
#include "samoa/log.hpp"
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>

namespace samoa {
namespace server {

using namespace boost::asio;

listener::listener(const context_ptr_t & context,
    const protocol_ptr_t & protocol)
 : core::tasklet<listener>(
        core::proactor::get_proactor()->serial_io_service()),
   _context(context),
   _protocol(protocol),
   _accept_sock(*get_io_service())
{
    std::string str_port = boost::lexical_cast<std::string>(
        context->get_server_port());

    set_tasklet_name("listener<" + \
        context->get_server_hostname() + ":" + str_port + ">");

    // build resolution query
    ip::tcp::resolver::query query(context->get_server_hostname(), str_port);

    // blocks, & throws on resolution failure
    ip::tcp::endpoint ep = *ip::tcp::resolver(
        *get_io_service()).resolve(query);

    // open & bind the listening socket
    _accept_sock.open(ep.protocol());
    _accept_sock.set_option(ip::tcp::acceptor::reuse_address(true));
    _accept_sock.bind(ep);
    _accept_sock.listen();

    LOG_DBG("");
}

listener::~listener()
{
    LOG_DBG("");
}

std::string listener::get_address()
{ return _accept_sock.local_endpoint().address().to_string(); }

unsigned short listener::get_port()
{ return _accept_sock.local_endpoint().port(); }

void listener::run_tasklet()
{
    // next connection to accept
    on_accept(boost::system::error_code());
}

void listener::halt_tasklet()
{
    LOG_DBG("");
    _accept_sock.close();
    _next_sock.reset();
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
        // Create a client to service the socket
        // Lifetime is managed by client's use in callbacks. Eg, it's
        //  auto-destroyed when it falls out of the event-loop
        client::ptr_t c(boost::make_shared<client>(
            _context, _protocol, _next_io_srv, _next_sock));

        _context->get_tasklet_group()->start_orphaned_tasklet(c);
    }

    // Next connection to accept
    _next_io_srv = core::proactor::get_proactor()->serial_io_service();
    _next_sock.reset(new ip::tcp::socket(*_next_io_srv));

    // Schedule call on accept
    _accept_sock.async_accept(*_next_sock, boost::bind(
            &listener::on_accept, shared_from_this(),
            boost::asio::placeholders::error));

    return;
}

}
}

