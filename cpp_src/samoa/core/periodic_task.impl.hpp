
#include "samoa/core/proactor.hpp"
#include "samoa/log.hpp"
#include <boost/bind.hpp>

namespace samoa {
namespace core {

template<typename Derived>
periodic_task<Derived>::periodic_task()
 : core::tasklet<Derived>(core::proactor::get_proactor()->serial_io_service()),
   _timer(*core::tasklet<Derived>::get_io_service()),
   _halted(false)
{ }

template<typename Derived>
void periodic_task<Derived>::next_cycle(
    const boost::posix_time::time_duration & interval_delay)
{
    if(!_halted)
    {
        // reset timer with new interval
        _timer.expires_from_now(interval_delay);
        _timer.async_wait(
            boost::bind(&periodic_task<Derived>::on_period, _1,
                typename periodic_task<Derived>::weak_ptr_t(
                    core::tasklet<Derived>::shared_from_this())));
    }
}

template<typename Derived>
void periodic_task<Derived>::run_tasklet()
{
    on_period(boost::system::error_code(),
        core::tasklet<Derived>::shared_from_this());
}

template<typename Derived>
void periodic_task<Derived>::halt_tasklet()
{
    LOG_DBG("HALT called on " << core::tasklet<Derived>::get_tasklet_name());
    _halted = true;
    _timer.cancel();
}

template<typename Derived>
void periodic_task<Derived>::on_period(const boost::system::error_code & ec,
    const typename core::tasklet<Derived>::weak_ptr_t & weak_p)
{
    if(ec)
    {
        // timer cancelled
        return;
    }

    typename core::tasklet<Derived>::ptr_t task = weak_p.lock();
    if(!task || task->_halted)
    {
        // tasklet has been destroyed, or halted
        return;
    }

    task->begin_cycle();
}

}
}

