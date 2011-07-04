#ifndef SAMOA_CORE_TASKLET_GROUP_HPP
#define SAMOA_CORE_TASKLET_GROUP_HPP

#include "samoa/core/fwd.hpp"
#include "samoa/spinlock.hpp"
#include <boost/smart_ptr/enable_shared_from_this.hpp>
#include <map>

namespace samoa {
namespace core {

class tasklet_group :
    public boost::enable_shared_from_this<tasklet_group>
{
public:

    typedef tasklet_group_ptr_t ptr_t;

    tasklet_group();

    /// Note that tasklets hold a strong-pointer to tasklet_group,
    ~tasklet_group();

    /// Posts a call to tasklet::halt_tasklet() on the tasklet's io_service
    void cancel_tasklet(const std::string & tasklet_name);

    /*!
     * For each held tasklet, posts a call to tasklet::halt_tasklet() on
     *  that tasklet's io_service.
     *
     * Further attempts to start tasklets on this group will fail with a warning
     */
    void cancel_group();

    /*!
     * Posts a call to tasklet_base::run_tasklet() on the tasklet's io_service,
     *  and internally tracks the tasklet by-name.
     *
     * The post is done by weak_ptr, which is checked for exiration prior
     *  to the tasklet_base::run_tasklet() call.
     */
    void start_managed_tasklet(const tasklet_base_ptr_t &);

    /*!
     * Posts a call to tasklet_base::run_tasklet() on the tasklet's io_service,
     *  and internally tracks the tasklet by-name.
     *
     * The post is done by shared_ptr, and the tasklet need not be
     *  externally referenced.
     */
    void start_orphaned_tasklet(const tasklet_base_ptr_t &);

private:

    friend class tasklet_base;

    void tasklet_destroyed(const std::string &);

    typedef std::map<std::string, tasklet_base_weak_ptr_t> tasklets_t;
    tasklets_t _tasklets;

    bool _cancelled;

    samoa::spinlock _lock;

    static void on_start_managed_tasklet(const tasklet_base_weak_ptr_t &);
    static void on_start_orphaned_tasklet(const tasklet_base_ptr_t &);
    static void on_cancel_tasklet(const tasklet_base_weak_ptr_t &);
};

}
}

#endif

