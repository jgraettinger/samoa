#ifndef SAMOA_CLIENT_SERVER_HPP
#define SAMOA_CLIENT_SERVER_HPP

#include "samoa/core/stream_protocol.hpp"
#include "samoa/core/proactor.hpp"
#include <boost/asio.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/function.hpp>
#include <memory>

namespace samoa {
namespace client {

class server :
    public boost::enable_shared_from_this<server>,
    public core::stream_protocol
{
public:

    typedef boost::shared_ptr<server> ptr_t;

    typedef boost::function<
        void(const boost::system::error_code &, server::ptr_t)
    > connect_to_callback_t;

    static void connect_to(core::proactor::ptr_t,
        const std::string & host, const std::string & port,
        const connect_to_callback_t &);

private:

    server(std::unique_ptr<boost::asio::ip::tcp::socket> &);

    void on_resolve(boost::system::error_code,
        boost::asio::ip::tcp::resolver::iterator,
        const connect_to_callback_t &);

    void on_connect(boost::system::error_code,
        const connect_to_callback_t &);

    void on_timeout(boost::system::error_code,
        const connect_to_callback_t &);

    boost::asio::ip::tcp::resolver _resolver;
    boost::asio::ip::tcp::resolver::iterator _endpoint_iter;
    boost::asio::deadline_timer _timer;
};

}
}

#endif

