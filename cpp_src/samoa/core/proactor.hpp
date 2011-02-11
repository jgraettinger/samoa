#ifndef SAMOA_CORE_PROACTOR_HPP
#define SAMOA_CORE_PROACTOR_HPP

#include <boost/enable_shared_from_this.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/thread.hpp>

namespace samoa {
namespace core {

typedef boost::shared_ptr<boost::asio::io_service> io_service_ptr_t;

class proactor :
    private boost::noncopyable,
    public boost::enable_shared_from_this<proactor>
{
public:

    typedef boost::shared_ptr<proactor> ptr_t;

    proactor();

    /* 
    *  Selects a single-threaded io_service from the pool of such
    *    io_services, in round-robin fashion. If no single-threaded
    *    io_services are available, the concurrent io_service is
    *    returned.
    *
    *  Non-threaded io_services are intended for use with non-blocking
    *    handlers, such as ones performing asynchronous network IO.
    *  Because the io_service is known to be single-threaded, no
    *    explicit synchronization is required for any handlers running
    *    on that service.
    */
    io_service_ptr_t select_serial_io_service()
    {
        boost::thread::lock_guard guard(_mutex);

        if(_serial_io_services.empty())
            return get_concurrent_io_service();

        io_service_ptr_t s(_serial_io_services.at(_next));
        _next = (_next + 1) % _serial_io_services.size();
        return s;
    }

    /*
    *  Selects a multi-threaded io_service. If no multi-threaded io_service
    *    is available, a single-threaded service is returned.
    *
    *  Threaded io_services are intended for use with blocking handlers,
    *    such as one performing synchronous disk IO.
    */
    io_service_ptr_t & get_concurrent_io_service()
    {
        return _threaded_io_service;
    }

    typedef boost::function<void ()> run_later_callback_t;
    void run_later(const run_later_callback_t &, unsigned delay_ms);

    void declare_serial_io_service();

    void declare_concurrent_io_service();

    void run();

private:

    unsigned _next;
    std::vector<io_service_ptr_t> _nonthreaded_io_services;

    io_service_ptr_t _threaded_io_service;

    boost::thread::thread_specific_ptr<boost::asio::io_service> _io_srv;
    boost::thread::mutex _mutex;
};

}
}

#endif

