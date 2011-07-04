
#include "samoa/log.hpp"
#include <boost/bind.hpp>

namespace samoa {
namespace server {

template<typename Derived>
periodic_task<Derived>::periodic_task(
    const context_ptr_t & context, unsigned target_period_ms)
 : core::tasklet<Derived>(
    core::proactor::get_proactor()->serial_io_service()),
   _context(context),
   _current_period_ms(target_period_ms),
   _target_period_ms(target_period_ms),
   _in_iteration(false),
   _timer(*core::tasklet<Derived>::get_io_service())
{
    SAMOA_ASSERT(_target_period_ms);
}

template<typename Derived>
void periodic_task<Derived>::end_iteration()
{
    _in_iteration = false;
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
    _current_period_ms = 0;
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
    if(!task || task->_current_period_ms == 0)
    {
        // tasklet has been destroyed, or halted
        return;
    }

    context_ptr_t ctxt = task->_context.lock();
    if(!ctxt)
    {
        // server context has been destroyed
        return;
    }

    if(task->_in_iteration)
    {
        // the current iteration hasn't yet completed;
        //   quadratically increase period-length
        LOG_WARN(task->get_tasklet_name() << " is in-iteration; doubling "
            "period from " << task->_current_period_ms << "ms to " \
             << (2 * task->_current_period_ms) << "ms");

        task->_current_period_ms *= 2;
    }

    // linearly shrink period-length, to minimum of _target_period_ms
    task->_current_period_ms = std::max(task->_target_period_ms,
        task->_current_period_ms - (unsigned)(0.1 * task->_target_period_ms));

    // schedule next iteration
    task->_timer.expires_from_now(
        boost::posix_time::milliseconds(task->_current_period_ms));
    task->_timer.async_wait(boost::bind(
        &periodic_task<Derived>::on_period, _1, weak_p));

    if(!task->_in_iteration)
    {
        task->_in_iteration = true;
        task->begin_iteration(ctxt);
    }
}

}
}

