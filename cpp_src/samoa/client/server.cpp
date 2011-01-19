
#include "samoa/client/server.hpp"
#include <iostream>

namespace samoa {
namespace client {

using namespace boost::asio::ip;
using namespace std;

server::server(std::unique_ptr<boost::asio::ip::tcp::socket> & sock)
 :  core::stream_protocol(sock),
    _resolver(socket().get_io_service()),
    _timer(socket().get_io_service())
{ }

void server::connect_to(core::proactor::ptr_t proactor,
    const string & host, const string & port,
    const server::connect_to_callback_t & callback)
{
    std::unique_ptr<tcp::socket> sock(new tcp::socket(
        proactor->get_nonblocking_io_service()));

    server::ptr_t srv(new server(sock));

    // start an async resolution of the host & port
    tcp::resolver::query query(host, port);
    srv->_resolver.async_resolve(query, boost::bind(
        &server::on_resolve, srv,
        boost::asio::placeholders::error,
        boost::asio::placeholders::iterator,
        callback));

    // set a new timeout of 60 seconds
    srv->_timer.expires_from_now(boost::posix_time::seconds(60));
    srv->_timer.async_wait(boost::bind(&server::on_timeout, srv,
        boost::asio::placeholders::error, callback));
}

void server::on_resolve(boost::system::error_code ec,
    tcp::resolver::iterator endpoint_iter,
    const server::connect_to_callback_t & callback)
{
    if(ec)
    {
        // resolution failed   
        callback(ec, server::ptr_t());
    }

    // resolution succeeded
    _endpoint_iter = endpoint_iter;

    on_connect(boost::asio::error::operation_aborted, callback);
}

void server::on_connect(boost::system::error_code ec,
    const server::connect_to_callback_t & callback)
{
    if(ec && _endpoint_iter != tcp::resolver::iterator())
    {
        socket().close();

        // attempt to connect to the next endpoint
        tcp::endpoint endpoint = *(_endpoint_iter++);
        socket().async_connect(endpoint, boost::bind(
            &server::on_connect, shared_from_this(),
            boost::asio::placeholders::error, callback));

        // set a new timeout of 60 seconds
        _timer.expires_from_now(boost::posix_time::seconds(60));
        _timer.async_wait(boost::bind(&server::on_timeout, shared_from_this(),
            boost::asio::placeholders::error, callback));
    }
    else if(ec)
    {
        // attempt failed, no more endpoints
        callback(ec, server::ptr_t());
    }
    else
    {
        // successfully connected
        _timer.cancel();
        callback(ec, shared_from_this());
    }
}

void server::on_timeout(boost::system::error_code ec,
    const server::connect_to_callback_t & callback)
{
    // was the timer canceled?
    if(ec == boost::asio::error::operation_aborted)
        return;

    // timeout: cancel any pending async operations
    _resolver.cancel();
    socket().cancel();
}

}
}

