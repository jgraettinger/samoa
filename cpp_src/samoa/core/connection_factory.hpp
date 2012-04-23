#ifndef SAMOA_CORE_CONNECTION_FACTORY_HPP
#define SAMOA_CORE_CONNECTION_FACTORY_HPP

#include "samoa/core/proactor.hpp"
#include <boost/enable_shared_from_this.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/asio.hpp>
#include <functional>

namespace samoa {
namespace core {

class connection_factory :
    public boost::enable_shared_from_this<connection_factory>
{
public:

    static unsigned connect_timeout_ms /* = 60000 */;

    typedef std::unique_ptr<boost::asio::ip::tcp::socket> socket_ptr_t;

    typedef std::function<
        void(boost::system::error_code, socket_ptr_t, io_service_ptr_t)
    > callback_t;

    connection_factory(callback_t);
    ~connection_factory();

    static void connect_to(
        callback_t callback,
        const std::string & host,
        unsigned short port);

private:

    typedef boost::shared_ptr<connection_factory> ptr_t;

    void on_connect(boost::system::error_code);

    void on_timeout(boost::system::error_code);

    io_service_ptr_t _io_srv;
    socket_ptr_t _sock;

    boost::asio::ip::tcp::resolver _resolver;
    boost::asio::ip::tcp::resolver::iterator _endpoint_iter;
    boost::asio::deadline_timer _timer;

    callback_t _callback;
};

};
};

#endif

