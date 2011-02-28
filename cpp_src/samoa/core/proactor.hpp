#ifndef SAMOA_CORE_PROACTOR_HPP
#define SAMOA_CORE_PROACTOR_HPP

#include <boost/enable_shared_from_this.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/thread.hpp>
#include <boost/thread/mutex.hpp>

namespace samoa {
namespace core {

class proactor :
    private boost::noncopyable,
    public boost::enable_shared_from_this<proactor>
{
public:

    typedef boost::shared_ptr<proactor> ptr_t;

    /*
    *  The thread invoking the proactor constructor implicitly
    *    declares itself as running a serial io-service
    */
    proactor();

    virtual ~proactor();

    /*
    *  Declares that this thread will run a serial
    *    (synchronous) io-service event loop
    */
    void declare_serial_io_service();

    /*
    *  Declares that this thread will run a concurrent
    *    (threaded) io-service event loop
    */
    void declare_concurrent_io_service();

    /* 
    *  Selects a single-threaded io_service from the pool of such
    *    io_services, in round-robin fashion.
    *
    *  Non-threaded io_services are intended for use with non-blocking
    *    handlers, such as ones performing asynchronous network IO.
    *  Because the io_service is known to be single-threaded, no
    *    explicit synchronization is required for any handlers running
    *    on that service.
    */
    boost::asio::io_service & serial_io_service();

    /*
    *  Selects a multi-threaded io_service. If no multi-threaded io_service
    *    is available, a single-threaded service is returned.
    *
    *  Threaded io_services are intended for use with blocking handlers,
    *    such as one performing synchronous disk IO.
    */
    boost::asio::io_service & concurrent_io_service();

    typedef boost::shared_ptr<boost::asio::deadline_timer> timer_ptr_t;
    typedef boost::function<void ()> run_later_callback_t;

    timer_ptr_t run_later(const run_later_callback_t &, unsigned delay_ms);

    void run(bool exit_when_idle);

    void shutdown();

private:

    boost::mutex _mutex;

    std::vector<boost::asio::io_service *> _serial_io_services;
    unsigned _next_serial_service;

    boost::asio::io_service _threaded_io_service;
    unsigned _concurrent_thread_count;

    boost::thread_specific_ptr<boost::asio::io_service> _io_srv;
};

}
}

#endif

