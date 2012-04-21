
#include "samoa/core/connection_factory.hpp"
#include <boost/smart_ptr/make_shared.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/asio.hpp>
#include <functional>

namespace samoa {
namespace core {

using namespace boost::asio;

unsigned connection_factory::connect_timeout_ms = 60000;

connection_factory::connection_factory()
 :  _sock(new ip::tcp::socket(*proactor::get_proactor()->serial_io_service())),
    _resolver(_sock->get_io_service()),
    _timer(_sock->get_io_service())
{ }

/* static */
connection_factory::ptr_t connection_factory::connect_to(
    callback_t callback,
    const std::string & host,
    unsigned short port)
{
    ptr_t self = boost::make_shared<connection_factory>();
    weak_ptr_t w_self = self;

    ip::tcp::resolver::query query(host,
        boost::lexical_cast<std::string>(port));

    // start an async resolution of the host & port
    self->_resolver.async_resolve(query,
        [w_self, callback = std::move(callback)](
            boost::system::error_code ec,
            ip::tcp::resolver::iterator endpoint_iter)
        {
            ptr_t self = w_self.lock();
            if(!self)
            {
                // destroyed during resolution operation
                LOG_INFO("post-dtor callback");
                return;
            }

            if(ec)
            {
                // resolution failed or timed out
                self->_timer.cancel();
                self->_sock->close();
                callback(ec, std::move(self->_sock));
            }
            else
            {
                // resolution succeeded
                self->_endpoint_iter = endpoint_iter;

                on_connect(std::move(w_self),
                    boost::asio::error::operation_aborted,
                    std::move(callback));
            }
        });

    // start a new timeout period
    self->_timer.expires_from_now(
        boost::posix_time::milliseconds(connect_timeout_ms));
    self->_timer.async_wait(std::bind(
        &connection_factory::on_timeout, w_self, std::placeholders::_1));
    return self;
}

/* static */
void connection_factory::on_connect(
    weak_ptr_t w_self,
    boost::system::error_code ec,
    connection_factory::callback_t callback)
{
    ptr_t self = w_self.lock();
    if(!self)
    {
        LOG_INFO("post-dtor callback");
        return;
    }
    if(ec && self->_endpoint_iter != ip::tcp::resolver::iterator())
    {
        self->_sock->close();

        // attempt to connect to the next endpoint
        ip::tcp::endpoint endpoint = *(self->_endpoint_iter++);
        self->_sock->async_connect(endpoint,
            [w_self, callback = std::move(callback)](
                boost::system::error_code ec)
            {
                on_connect(std::move(w_self), ec, std::move(callback));
            });

        // start a new timeout period
        self->_timer.expires_from_now(
            boost::posix_time::milliseconds(connect_timeout_ms));
        self->_timer.async_wait(std::bind(
            &connection_factory::on_timeout, w_self, std::placeholders::_1));
    }
    else if(ec)
    {
        // attempt failed, no more endpoints
        self->_timer.cancel();
        self->_sock->close();
        callback(ec, std::move(self->_sock));
    }
    else
    {
        // successfully connected
        self->_timer.cancel();

        // we internally buffer requests already when building them,
        //   and don't want additional Nagle delay
        self->_sock->set_option(ip::tcp::no_delay(true));

        callback(ec, std::move(self->_sock));
    }
}

/* static */
void connection_factory::on_timeout(
    weak_ptr_t w_self,
    boost::system::error_code ec)
{
    ptr_t self = w_self.lock();
    if(!self)
    {
        LOG_INFO("post-dtor callback");
        return;
    }

    // was the timer canceled?
    if(ec == boost::asio::error::operation_aborted)
        return;

    // timeout: cancel any pending async operations
    //  this results in completion handlers being called
    //  with an error, so no need to issue a callback here
    self->_resolver.cancel();
    self->_sock->cancel();
}

}
}

