
#include "samoa/core/proactor.hpp"
#include <boost/smart_ptr/make_shared.hpp>

namespace samoa {
namespace core {

void null_cleanup(boost::asio::io_service *){}

// boost::asio uses RTTI to establish a common registry of services,
//  however RTTI doesn't work well across shared library boundaries.
//  placing the ctor here guarantees that the member _io_services is
//  constructed in the context of libsamoa
proactor::proactor()
 : _next_serial_service(0),
   _concurrent_thread_count(0),
   _io_srv(&null_cleanup)
{ declare_serial_io_service(); }

proactor::~proactor()
{
    for(size_t i = 0; i != _serial_io_services.size(); ++i)
        delete _serial_io_services[i];
}

void proactor::declare_serial_io_service()
{
    boost::mutex::scoped_lock guard(_mutex);

    if(_io_srv.get())
        throw std::runtime_error("proactor::declare_serial_io_service(): "
            "this thread has already declared");

    // create a new io-service to run
    _serial_io_services.push_back(new boost::asio::io_service());

    _io_srv.reset(_serial_io_services.back());
}

void proactor::declare_concurrent_io_service()
{
    boost::mutex::scoped_lock guard(_mutex);

    if(_io_srv.get())
        throw std::runtime_error("proactor::declare_concurrent_io_service(): "
            "this thread has already declared");

    assert(!_serial_io_services.empty()); 

    _concurrent_thread_count += 1;

    _io_srv.reset(&_threaded_io_service);
}

boost::asio::io_service & proactor::serial_io_service()
{
    boost::mutex::scoped_lock guard(_mutex);

    assert(!_serial_io_services.empty()); 

    boost::asio::io_service & io_srv(
        *_serial_io_services.at(_next_serial_service));

    // round-robin increment 
    _next_serial_service = \
        (_next_serial_service + 1) % _serial_io_services.size();

    return io_srv;
}

boost::asio::io_service & proactor::concurrent_io_service()
{
    boost::mutex::scoped_lock guard(_mutex);

    if(!_concurrent_thread_count)
        return serial_io_service();

    return _threaded_io_service;
}

// helper for proactor::run_later callback dispatch
void on_run_later(const boost::system::error_code & ec,
    const proactor::run_later_callback_t & callback,
    const proactor::timer_ptr_t &)
{
    // if cancelled
    if(ec)
    { return; }

    callback();
}

// Again, deadline_timer doesn't work across shared-library boundaries,
//  so it's use needs to be encapsulated into libsamoa.
proactor::timer_ptr_t proactor::run_later(
    const proactor::run_later_callback_t & callback, unsigned delay_ms)
{
    if(!_io_srv.get())
        throw std::runtime_error("proactor::run_later(): "
            "declare_serial_io_service or declare_concurrent_io_service must "
            "first be called from this thread");

    timer_ptr_t timer = boost::make_shared<boost::asio::deadline_timer>(*_io_srv);

    timer->expires_from_now(boost::posix_time::milliseconds(delay_ms));

    // timer reference count is passed to boost::bind callback argument
    timer->async_wait(boost::bind(&on_run_later,
        boost::asio::placeholders::error, callback, timer));

    return timer;
}

void proactor::run(bool exit_when_idle /* = true */)
{
    if(!_io_srv.get())
        throw std::runtime_error("proactor::run(): "
            "declare_serial_io_service or declare_concurrent_io_service must "
            "first be called from this thread");

//    std::unique_ptr<boost::asio::io_service::work> work;

//    if(!exit_when_idle)
//        work.reset(new boost::asio::io_service::work(*_io_srv));

    _io_srv->run();
    // clean exit => no more work
    _io_srv->reset();
}

void proactor::shutdown()
{
    boost::mutex::scoped_lock guard(_mutex);

    for(size_t i = 0; i != _serial_io_services.size(); ++i)
        _serial_io_services[i]->stop();

    _threaded_io_service.stop();
    return;
}

}
}

