
#include "samoa/core/proactor.hpp"
#include "samoa/error.hpp"
#include "samoa/log.hpp"
#include <boost/bind.hpp>

namespace samoa {
namespace core {

// lifetime is already managed by shared_ptr
void null_cleanup(boost::asio::io_service *){}

// static initialization
boost::weak_ptr<proactor> proactor::_class_instance;
spinlock proactor::_class_lock;

// boost::asio uses RTTI to establish a common registry of services,
//  however RTTI doesn't work well across shared library boundaries.
//  placing the ctor here guarantees that the member _io_services is
//  constructed in the context of libsamoa
proactor::proactor()
 : _next_serial_service(0),
   _concurrent_thread_count(0),
   _io_srv(&null_cleanup)
{
    LOG_DBG("called");
}

proactor::~proactor()
{
    LOG_DBG("called");
}

void proactor::declare_serial_io_service()
{
    spinlock::guard guard(_class_lock);

    SAMOA_ASSERT(!_io_srv.get() && "this thread has already declared");

    LOG_DBG("serial service declared");

    // create a new io-service to run
    _serial_io_services.push_back(
        io_service_ptr_t(new boost::asio::io_service()));

    _io_srv.reset(_serial_io_services.back().get());
}

void proactor::declare_concurrent_io_service()
{
    spinlock::guard guard(_class_lock);

    SAMOA_ASSERT(!_io_srv.get() && "this thread has already declared");

    SAMOA_ASSERT(!_serial_io_services.empty());

    LOG_DBG("concurrent service declared");

    _concurrent_thread_count += 1;

    if(!_threaded_io_service)
        _threaded_io_service.reset(new boost::asio::io_service());

    _io_srv.reset(_threaded_io_service.get());
}

io_service_ptr_t proactor::serial_io_service()
{
    spinlock::guard guard(_class_lock);

    SAMOA_ASSERT(!_serial_io_services.empty());

    io_service_ptr_t io_srv = _serial_io_services.at(_next_serial_service); 

    // round-robin increment 
    _next_serial_service = \
        (_next_serial_service + 1) % _serial_io_services.size();

    return io_srv;
}

io_service_ptr_t proactor::concurrent_io_service()
{
    _class_lock.acquire();

    if(!_threaded_io_service)
    {
        _class_lock.release();
        return serial_io_service();
    }

    io_service_ptr_t result = _threaded_io_service;
    _class_lock.release();

    return result;
}

// helper for proactor::run_later callback dispatch
void on_run_later(const boost::system::error_code & ec,
    const proactor::run_later_callback_t & callback,
    const io_service_ptr_t & io_srv,
    const timer_ptr_t &)
{
    // if cancelled
    if(ec)
    { return; }

    callback(io_srv);
}

// deadline_timer doesn't work across shared-library boundaries,
//  so it's use needs to be encapsulated into libsamoa.
timer_ptr_t proactor::run_later(const run_later_callback_t & callback,
    unsigned delay_ms)
{
    io_service_ptr_t io_srv = serial_io_service();

    timer_ptr_t timer = boost::make_shared<boost::asio::deadline_timer>(*io_srv);
    timer->expires_from_now(boost::posix_time::milliseconds(delay_ms));

    // timer reference count is passed to boost::bind callback argument
    timer->async_wait(boost::bind(&on_run_later,
        _1, callback, io_srv, timer));

    return timer;
}

void proactor::shutdown()
{
    spinlock::guard guard(_class_lock);

    for(size_t i = 0; i != _serial_io_services.size(); ++i)
        _serial_io_services[i]->stop();

    if(_threaded_io_service)
        _threaded_io_service->stop();
}

}
}

