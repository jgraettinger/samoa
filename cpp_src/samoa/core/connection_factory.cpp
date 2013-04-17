
#include "samoa/core/connection_factory.hpp"
#include "samoa/log.hpp"
#include <boost/lexical_cast.hpp>
#include <boost/asio.hpp>
#include <functional>
#include <memory>

namespace samoa {
namespace core {

using namespace boost::asio;

unsigned connection_factory::connect_timeout_ms = 60000;

connection_factory::connection_factory(callback_t callback)
 :  _io_srv(proactor::get_proactor()->serial_io_service()),
    _sock(new ip::tcp::socket(*_io_srv)),
    _resolver(*_io_srv),
    _timer(*_io_srv),
    _callback(std::move(callback))
{ }

connection_factory::~connection_factory()
{
    LOG_INFO("destroyed");
}

/* static */
void connection_factory::connect_to(
    callback_t callback,
    const std::string & host,
    unsigned short port)
{
    ptr_t self = std::make_shared<connection_factory>(
        std::move(callback));

    ip::tcp::resolver::query query(host,
        boost::lexical_cast<std::string>(port));

    // start an async resolution of the host & port
    self->_resolver.async_resolve(query,
        [self](boost::system::error_code ec,
            ip::tcp::resolver::iterator endpoint_iter)
        {
            if(ec)
            {
                // resolution failed or timed out
                self->_timer.cancel();
                self->_sock->close();
                self->_callback(ec, socket_ptr_t(), io_service_ptr_t());
                self->_callback = callback_t();
            }
            else
            {
                // resolution succeeded
                self->_endpoint_iter = endpoint_iter;

                self->on_connect(boost::asio::error::operation_aborted);
            }
        });

    // start a new timeout period
    self->_timer.expires_from_now(
        boost::posix_time::milliseconds(connect_timeout_ms));
    self->_timer.async_wait(
        std::bind(&connection_factory::on_timeout, self,
            std::placeholders::_1));
}

void connection_factory::on_connect(boost::system::error_code ec)
{
    if(ec && _endpoint_iter != ip::tcp::resolver::iterator())
    {
        _sock->close();

        // attempt to connect to the next endpoint
        ip::tcp::endpoint endpoint = *(_endpoint_iter++);
        _sock->async_connect(endpoint,
            std::bind(&connection_factory::on_connect, shared_from_this(),
                std::placeholders::_1));

        // start a new timeout period
        _timer.expires_from_now(
            boost::posix_time::milliseconds(connect_timeout_ms));
        _timer.async_wait(
            std::bind(&connection_factory::on_timeout, shared_from_this(),
                std::placeholders::_1));
    }
    else if(ec)
    {
        // attempt failed, no more endpoints
        _timer.cancel();
        _sock->close();
        _callback(ec, socket_ptr_t(), io_service_ptr_t());
        _callback = callback_t();
    }
    else
    {
        // successfully connected
        _timer.cancel();

        // we internally buffer requests already when building them,
        //   and don't want additional Nagle delay
        _sock->set_option(ip::tcp::no_delay(true));

        LOG_INFO("successfully connected");
        _callback(ec, std::move(_sock), std::move(_io_srv));
        _callback = callback_t();
    }
}

void connection_factory::on_timeout(boost::system::error_code ec)
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

