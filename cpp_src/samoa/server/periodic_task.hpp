#ifndef SAMOA_SERVER_PERIODIC_TASK_HPP
#define SAMOA_SERVER_PERIODIC_TASK_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/client/server.hpp"
#include "samoa/core/fwd.hpp"
#include "samoa/core/tasklet.hpp"
#include "samoa/core/uuid.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include <boost/asio.hpp>

namespace samoa {
namespace server {

template<typename Derived>
class periodic_task : public core::tasklet<Derived>
{
public:

    using core::tasklet<Derived>::ptr_t;
    using core::tasklet<Derived>::weak_ptr_t;

    periodic_task(const context_ptr_t &, unsigned target_period_ms);

protected:

    /*! Static polymorphism: subclasses must define
    *     void begin_iteration(const context_ptr_t &);
    */

    void end_iteration();

private:

    void run_tasklet();
    void halt_tasklet();

    static void on_period(const boost::system::error_code &,
        const typename core::tasklet<Derived>::weak_ptr_t &);

    const context_weak_ptr_t _context;
    unsigned _current_period_ms;
    unsigned _target_period_ms;
    bool _in_iteration;

    boost::asio::deadline_timer _timer;
};

}
}

#include "samoa/server/periodic_task.impl.hpp"

#endif

