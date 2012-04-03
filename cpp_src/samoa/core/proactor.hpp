#ifndef SAMOA_CORE_PROACTOR_HPP
#define SAMOA_CORE_PROACTOR_HPP

#include "samoa/core/fwd.hpp"
#include "samoa/spinlock.hpp"
#include <boost/asio.hpp>
#include <boost/function.hpp>
#include <boost/thread/tss.hpp>

namespace samoa {
namespace core {

class proactor :
    public boost::enable_shared_from_this<proactor>
{
public:

    typedef proactor_ptr_t ptr_t;

    typedef boost::function<void()> run_later_callback_t;

    /*!
    * Reference-counted singleton
    *  Only one proactor instance exists at a time, but the instance
    *  will be destroyed when the last client-held pointer goes out of
    *  scope.
    *
    * The first thread to call get_proactor() implicitly calls
    *  declare_serial_io_service()
    */
    static proactor::ptr_t get_proactor()
    {
        ptr_t result;
        {
            spinlock::guard guard(_class_lock);

            result = _class_instance.lock();

            if(result)
                return result;

            result = ptr_t(new proactor());
            _class_instance = result;
        }
        result->declare_serial_io_service();
        return result;
    }

    virtual ~proactor();

    /*!
    *  Declares that this thread will run a serial
    *    (synchronous) io-service event loop
    */
    void declare_serial_io_service();

    /*!
    *  Declares that this thread will run a concurrent
    *    (threaded) io-service event loop
    */
    void declare_concurrent_io_service();

    /*!
    *  Returns the io-service declared on this thread
    */
    boost::asio::io_service * get_declared_io_service()
    { return _io_srv.get(); }

    /*!
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

    /*!
    *  Selects a multi-threaded io_service. If no multi-threaded io_service
    *    is available, a single-threaded service is returned.
    *
    *  Threaded io_services are intended for use with blocking handlers,
    *    such as one performing synchronous disk IO.
    */
    io_service_ptr_t concurrent_io_service();

    /*!
    *  Schedules a callable to be invoked at a future time on a serial
    *   io-service.
    *
    *  TODO(johng): Iff delay_ms = 0, the callable will be immediately
    *   posted to the io-service.
    *
    *  TODO(johng): boost::asio::deadline_timer doesn't work across shared-library
    *   boundaries, so it's use has been encapsulated within libsamoa. according
    *   to docs, shared-libraries shouldn't be a problem-- this needs another pass
    */
    timer_ptr_t run_later(const run_later_callback_t &, unsigned delay_ms);

    void shutdown();

private:

    proactor();

    static boost::weak_ptr<proactor> _class_instance;
    static spinlock _class_lock;

    std::vector<io_service_ptr_t> _serial_io_services;
    unsigned _next_serial_service;

    io_service_ptr_t _threaded_io_service;
    unsigned _concurrent_thread_count;

    boost::thread_specific_ptr<boost::asio::io_service> _io_srv;
};

}
}

#endif

