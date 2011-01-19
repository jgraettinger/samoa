#ifndef SAMOA_CORE_PROACTOR_HPP
#define SAMOA_CORE_PROACTOR_HPP

#include <boost/enable_shared_from_this.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/function.hpp>

namespace samoa {
namespace core {

class proactor :
    private boost::noncopyable,
    public boost::enable_shared_from_this<proactor>
{
public:

    typedef boost::shared_ptr<proactor> ptr_t;

    proactor();

    typedef boost::function<void ()> run_later_callback_t;
    void run_later(const run_later_callback_t &, unsigned delay_ms);

    boost::asio::io_service & get_nonblocking_io_service()
    { return _io_service; }

    boost::asio::io_service & get_blocking_io_service()
    { return _io_service; }

private:

    boost::asio::io_service _io_service;
};

}
}

#endif

