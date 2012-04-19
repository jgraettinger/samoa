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

    typedef boost::shared_ptr<connection_factory> ptr_t;
    typedef boost::weak_ptr<connection_factory> weak_ptr_t;

    connection_factory();

    typedef std::function<
        void(const boost::system::error_code & ec,
            const io_service_ptr_t &,
            std::unique_ptr<boost::asio::ip::tcp::socket> &)
    > callback_t;

    static ptr_t connect_to(
        callback_t callback,
        const std::string & host,
        unsigned short port);

private:

    void on_resolve(const boost::system::error_code &,
        const boost::asio::ip::tcp::resolver::iterator &,
        const callback_t &);

    void on_connect(const boost::system::error_code &,
        const callback_t &);

    void on_timeout(const boost::system::error_code &);

    std::unique_ptr<boost::asio::ip::tcp::socket> _sock;

    boost::asio::ip::tcp::resolver _resolver;
    boost::asio::ip::tcp::resolver::iterator _endpoint_iter;
    boost::asio::deadline_timer _timer;
};

};
};

#endif

