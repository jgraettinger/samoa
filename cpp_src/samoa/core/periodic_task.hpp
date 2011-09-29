#ifndef SAMOA_CORE_PERIODIC_TASK_HPP
#define SAMOA_CORE_PERIODIC_TASK_HPP

#include "samoa/core/fwd.hpp"
#include "samoa/core/tasklet.hpp"
#include <boost/asio.hpp>

namespace samoa {
namespace core {

template<typename Derived>
class periodic_task : public core::tasklet<Derived>
{
public:

    using core::tasklet<Derived>::ptr_t;
    using core::tasklet<Derived>::weak_ptr_t;

    using core::tasklet<Derived>::get_io_service;

    periodic_task();

protected:

    /*! Static polymorphism: subclasses must define
     *   void begin_cycle();
     */
    
    /*!
     * Notifies periodic_task that the cycle has finished
     *
     * @param delay Target interval until next cycle
     */
    void end_cycle(const boost::posix_time::time_duration & interval);

private:

    void run_tasklet();
    void halt_tasklet();

    static void on_period(const boost::system::error_code &,
        const typename core::tasklet<Derived>::weak_ptr_t &);

    boost::asio::deadline_timer _timer;
    bool _halted;
};

}
}

#include "samoa/core/periodic_task.impl.hpp"

#endif

