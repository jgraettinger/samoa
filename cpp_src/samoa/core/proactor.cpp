
#include "samoa/core/proactor.hpp"

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
{ }

void proactor::declare_serial_io_service()
{
    std::lock_guard<std::mutex> guard(_mutex);

    if(_io_srv.get())
        throw std::runtime_error("proactor::declare_serial_io_service(): "
            "this thread has already declared");

    // create a new io-service to run
    _serial_io_services.push_back(
        io_service_ptr_t(new boost::asio::io_service()));

    _io_srv.reset(_serial_io_services.back().get());
}

void proactor::declare_concurrent_io_service()
{
    std::lock_guard<std::mutex> guard(_mutex);

    if(_io_srv.get())
        throw std::runtime_error("proactor::declare_concurrent_io_service(): "
            "this thread has already declared");

    assert(!_serial_io_services.empty()); 

    _concurrent_thread_count += 1;

    if(!_threaded_io_service)
        _threaded_io_service.reset(new boost::asio::io_service());

    _io_srv.reset(_threaded_io_service.get());
}

io_service_ptr_t proactor::serial_io_service()
{
    std::lock_guard<std::mutex> guard(_mutex);

    assert(!_serial_io_services.empty()); 

    io_service_ptr_t io_srv = _serial_io_services.at(_next_serial_service); 

    // round-robin increment 
    _next_serial_service = \
        (_next_serial_service + 1) % _serial_io_services.size();

    return io_srv;
}

io_service_ptr_t proactor::concurrent_io_service()
{
    if(!_threaded_io_service)
        return serial_io_service();

    return _threaded_io_service;
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

// Again, deadline_timer doesn't work across shared-library boundaries,
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
    std::lock_guard<std::mutex> guard(_mutex);

    for(size_t i = 0; i != _serial_io_services.size(); ++i)
        _serial_io_services[i]->stop();

    if(_threaded_io_service)
        _threaded_io_service->stop();
}

}
}

