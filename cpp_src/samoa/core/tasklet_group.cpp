
#include "samoa/core/tasklet_group.hpp"
#include "samoa/core/tasklet.hpp"
#include "samoa/log.hpp"
#include "samoa/error.hpp"
#include <boost/bind.hpp>

namespace samoa {
namespace core {

tasklet_group::tasklet_group()
 : _cancelled(false)
{ }

tasklet_group::~tasklet_group()
{ }

void tasklet_group::cancel_tasklet(const std::string & tlet_name)
{
    samoa::spinlock::guard guard(_lock);

    tasklets_t::iterator it = _tasklets.find(tlet_name);
    SAMOA_ASSERT(it != _tasklets.end())

    tasklet_base::ptr_t tlet = it->second.lock();

    if(tlet)
    {
        tlet->get_io_service()->post(boost::bind(
            &tasklet_group::on_cancel_tasklet, it->second));
    }
}

void tasklet_group::cancel_group()
{
    samoa::spinlock::guard guard(_lock);
    _cancelled = true;

    for(tasklets_t::iterator it = _tasklets.begin();
        it != _tasklets.end(); ++it)
    {
        tasklet_base::ptr_t tlet = it->second.lock();

        if(tlet)
        {
            tlet->get_io_service()->post(boost::bind(
                &tasklet_group::on_cancel_tasklet, it->second));
        }
    }
}

void tasklet_group::start_managed_tasklet(const tasklet_base::ptr_t & tlet)
{
    SAMOA_ASSERT(!tlet->_group);
    SAMOA_ASSERT(!tlet->get_tasklet_name().empty());

    samoa::spinlock::guard guard(_lock);

    if(_cancelled)
    {
        LOG_WARN("attempt to start tasklet " << tlet->get_tasklet_name() << \
            " on a cancelled tasklet_group");
        return;
    }

    // see note in tasklet.hpp for the rationale here
    tasklet_base::weak_ptr_t weak_tlet = \
        tlet->shared_tasklet_base_from_this();

    SAMOA_ASSERT(_tasklets.insert(std::make_pair(
        tlet->get_tasklet_name(), weak_tlet)).second);

    tlet->_group = shared_from_this();

    tlet->get_io_service()->post(boost::bind(
        &tasklet_group::on_start_managed_tasklet, weak_tlet));
}

void tasklet_group::start_orphaned_tasklet(const tasklet_base::ptr_t & tlet)
{
    SAMOA_ASSERT(!tlet->_group);
    SAMOA_ASSERT(!tlet->get_tasklet_name().empty());

    samoa::spinlock::guard guard(_lock);

    if(_cancelled)
    {
        LOG_WARN("attempt to start tasklet " << tlet->get_tasklet_name() << \
            " on a cancelled tasklet_group");
        return;
    }

    // see note in tasklet.hpp for the rationale here
    tasklet_base::weak_ptr_t weak_tlet = \
        tlet->shared_tasklet_base_from_this();

    SAMOA_ASSERT(_tasklets.insert(std::make_pair(
        tlet->get_tasklet_name(), weak_tlet)).second);

    tlet->_group = shared_from_this();

    tlet->get_io_service()->post(boost::bind(
        &tasklet_group::on_start_orphaned_tasklet, tlet));
}

void tasklet_group::tasklet_destroyed(const std::string & name)
{
    samoa::spinlock::guard guard(_lock);

    tasklets_t::iterator it = _tasklets.find(name);
    SAMOA_ASSERT(it != _tasklets.end());
    _tasklets.erase(it);

    LOG_DBG(name);
}

void tasklet_group::on_start_managed_tasklet(
    const tasklet_base::weak_ptr_t & weak_tlet)
{
    tasklet_base::ptr_t tlet = weak_tlet.lock();
    if(tlet)
    {
        tlet->run_tasklet();
    }
    else
    {
        LOG_DBG("weak_tlet.expired()");
    }
}

void tasklet_group::on_start_orphaned_tasklet(
    const tasklet_base::ptr_t & tlet)
{
    tlet->run_tasklet();
}

void tasklet_group::on_cancel_tasklet(
    const tasklet_base::weak_ptr_t & weak_tlet)
{
    // silently ignore tasklets which have already been destroyed
    tasklet_base::ptr_t tlet = weak_tlet.lock();
    if(tlet)
    {
        tlet->halt_tasklet();
    }
}

}
}

