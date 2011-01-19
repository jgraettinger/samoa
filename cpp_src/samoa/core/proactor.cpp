
#include "samoa/core/proactor.hpp"

namespace samoa {
namespace core {

typedef boost::shared_ptr<boost::asio::deadline_timer> timer_ptr_t;

// helper for proactor::run_later callback dispatch
void on_run_later(const boost::system::error_code & ec,
    const proactor::run_later_callback_t & callback,
    const timer_ptr_t &)
{
    // if cancelled
    if(ec)
    { return; }

    callback();
}

// boost::asio uses RTTI to establish a common registry of services,
//  however RTTI doesn't work well across shared library boundaries.
//  placing the ctor here guarantees that the member _io_services is
//  constructed in the context of libsamoa
proactor::proactor()
{ }

// Again, deadline_timer doesn't work across shared-library boundaries,
//  so it's use needs to be encapsulated into libsamoa.
void proactor::run_later(const proactor::run_later_callback_t & callback,
    unsigned delay_ms)
{
    if(delay_ms)
    {
        timer_ptr_t timer(new boost::asio::deadline_timer(
            get_nonblocking_io_service()));

        timer->expires_from_now(boost::posix_time::milliseconds(delay_ms));

        // timer ownership is passed to boost::bind callback argument
        timer->async_wait(boost::bind(&on_run_later,
            boost::asio::placeholders::error, callback, timer));
    }
    else
    {
        get_nonblocking_io_service().post(boost::bind(&on_run_later,
            boost::system::error_code(), callback, timer_ptr_t()));
    }
}

}
}

