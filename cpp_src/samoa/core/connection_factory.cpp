
#include "samoa/core/connection_factory.hpp"
#include <boost/smart_ptr/make_shared.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/asio.hpp>
#include <boost/bind.hpp>

namespace samoa {
namespace core {

using namespace boost::asio;

// private constructor-class for use with boost::make_shared
class connection_factory_priv : public connection_factory
{
public:

    connection_factory_priv(const core::io_service_ptr_t & io_srv,
         unsigned t)
     : connection_factory(io_srv, t)
    { }
};

connection_factory::connection_factory(
    const core::io_service_ptr_t & io_srv, unsigned timeout_ms)
 :  _timeout_ms(timeout_ms),
    _io_srv(io_srv),
    _sock(new ip::tcp::socket(*io_srv)),
    _resolver(_sock->get_io_service()),
    _timer(_sock->get_io_service())
{ }

/* static */ connection_factory::ptr_t connection_factory::connect_to(
    const connection_factory::callback_t & callback,
    const std::string & host,
    unsigned short port)
{
    core::io_service_ptr_t io_srv = \
        proactor::get_proactor()->serial_io_service();

    ptr_t p(boost::make_shared<connection_factory_priv>(io_srv, 60000));

    ip::tcp::resolver::query query(host,
        boost::lexical_cast<std::string>(port));

    // start an async resolution of the host & port
    p->_resolver.async_resolve(query, boost::bind(
        &connection_factory::on_resolve, p,
        boost::asio::placeholders::error,
        boost::asio::placeholders::iterator,
        callback));

    // start a new timeout period
    p->_timer.expires_from_now(
        boost::posix_time::milliseconds(p->_timeout_ms));
    p->_timer.async_wait(boost::bind(
        &connection_factory::on_timeout, p,
        boost::asio::placeholders::error, callback));
    return p;
}

void connection_factory::on_resolve(
    const boost::system::error_code & ec,
    const ip::tcp::resolver::iterator & endpoint_iter,
    const connection_factory::callback_t & callback)
{
    if(ec)
    {
        // resolution failed or timed out
        _timer.cancel();
        _sock->close();
        callback(ec, io_service_ptr_t(), _sock);
    }

    // resolution succeeded
    _endpoint_iter = endpoint_iter;

    on_connect(boost::asio::error::operation_aborted, callback);
}

void connection_factory::on_connect(
    const boost::system::error_code & ec,
    const connection_factory::callback_t & callback)
{
    if(ec && _endpoint_iter != ip::tcp::resolver::iterator())
    {
        _sock->close();

        // attempt to connect to the next endpoint
        ip::tcp::endpoint endpoint = *(_endpoint_iter++);
        _sock->async_connect(endpoint, boost::bind(
            &connection_factory::on_connect, shared_from_this(),
            boost::asio::placeholders::error, callback));

        // start a new timeout period
        _timer.expires_from_now(boost::posix_time::milliseconds(_timeout_ms));
        _timer.async_wait(boost::bind(
            &connection_factory::on_timeout, shared_from_this(),
            boost::asio::placeholders::error, callback));
    }
    else if(ec)
    {
        // attempt failed, no more endpoints
        _timer.cancel();
        _sock->close();
        callback(ec, io_service_ptr_t(), _sock);
    }
    else
    {
        // successfully connected
        _timer.cancel();

        // we internally buffer requests already when building them,
        //   and don't want additional Nagle delay
        _sock->set_option(ip::tcp::no_delay(true));

        callback(ec, _io_srv, _sock);
    }
}

void connection_factory::on_timeout(
    const boost::system::error_code & ec,
    const connection_factory::callback_t & callback)
{
    // was the timer canceled?
    if(ec == boost::asio::error::operation_aborted)
        return;

    // timeout: cancel any pending async operations
    //  this results in completion handlers being called
    //  with an error, so no need to issue a callback here
    _resolver.cancel();
    _sock->cancel();
}

}
}

