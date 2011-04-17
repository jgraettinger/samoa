#ifndef SAMOA_CORE_PROACTOR_HPP
#define SAMOA_CORE_PROACTOR_HPP

#include "samoa/core/fwd.hpp"
#include <boost/asio.hpp>
#include <boost/function.hpp>

#include <boost/thread.hpp>
#include <mutex>

namespace samoa {
namespace core {

class proactor :
    public boost::enable_shared_from_this<proactor>
{
public:

    typedef proactor_ptr_t ptr_t;

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
    io_service_ptr_t serial_io_service();

    /*
    *  Selects a multi-threaded io_service. If no multi-threaded io_service
    *    is available, a single-threaded service is returned.
    *
    *  Threaded io_services are intended for use with blocking handlers,
    *    such as one performing synchronous disk IO.
    */
    io_service_ptr_t concurrent_io_service();


    typedef boost::function<
        void (const io_service_ptr_t &)
    > run_later_callback_t;

    timer_ptr_t run_later(const run_later_callback_t &, unsigned delay_ms);

    void run(bool exit_when_idle);

    void shutdown();

private:

    std::vector<io_service_ptr_t> _serial_io_services;
    unsigned _next_serial_service;

    io_service_ptr_t _threaded_io_service;
    unsigned _concurrent_thread_count;

    std::mutex _mutex;
    boost::thread_specific_ptr<boost::asio::io_service> _io_srv;
};

}
}

#endif

