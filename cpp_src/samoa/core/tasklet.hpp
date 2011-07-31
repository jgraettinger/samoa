#ifndef SAMOA_CORE_TASKLET_HPP
#define SAMOA_CORE_TASKLET_HPP

#include "samoa/core/fwd.hpp"

namespace samoa {
namespace core {

/*
A note on tasklets, shared_ptr, and boost::python:

tasklet_group tracks tasklets by weak_ptr, to allow for tasklets
to naturally be destroyed when all references are dropped.

All boost::python tasklet bindings use shared_ptr as the held-type,
so boost::python does maintain a shared_ptr to each tasklet.

_However_, boost::python uses the custom-deleter funtionality of
shared_ptr to hold references to the /python/ type, in such a way
that the original python instance can be recovered if the shared_ptr
is passed back to python.

In order to instrument this, when boost::python needs a shared_ptr
rvalue, it increments the /python/ reference-count, builds an entirely
new shared_ptr (including reference-count block), and sets the shared_ptr
deleter to simply decrement the /python/ reference-count.

Importantly, this chaining of shared_ptr reference counts to python reference
counts means that a shared_ptr reference count of zero may not mean the
instance was destroyed. This effectively breaks weak_ptrs, which tasklet_group
depends on.

The solution instrumented here is to require boost::python's held shared_ptr
is used when deriving weak_ptrs for tasklet_group, rather than using
boost::python's on-the-fly shared_ptr conversion.

Specifically, the curiously recurring template pattern is used to ensure
tasklet subclasses derive from boost::enable_shared_from_this<Derived>,
and that tasklets implement shared_tasklet_base_from_this(), which accesses
boost::python's held-type shared_ptr to obtain a tasklet_base::ptr_t.

tasklet_group then uses this method to obtain a weak_ptr to the instance.
*/

class tasklet_base
{
public:

    typedef tasklet_base_ptr_t ptr_t;
    typedef tasklet_base_weak_ptr_t weak_ptr_t;

    tasklet_base(const io_service_ptr_t &);

    virtual ~tasklet_base();

    const io_service_ptr_t & get_io_service()
    { return _io_service; }

    /// Set only when the tasklet has been started
    const tasklet_group_ptr_t & get_tasklet_group()
    { return _group; }

    const std::string & get_tasklet_name()
    { return _name; }

    /// Must be called exactly once, prior to starting the tasklet
    void set_tasklet_name(std::string &&);

protected:

    virtual void run_tasklet() = 0;
    virtual void halt_tasklet() = 0;
    virtual ptr_t shared_tasklet_base_from_this() = 0;

private:

    friend class tasklet_group;

    // proactor lifetime management
    const core::proactor_ptr_t _proactor;
    const core::io_service_ptr_t _io_service;

    tasklet_group_ptr_t _group;
    std::string _name;
};

template<typename Derived>
class tasklet :
    public tasklet_base,
    public boost::enable_shared_from_this<Derived>
{
public:

    typedef boost::shared_ptr<Derived> ptr_t;
    typedef boost::weak_ptr<Derived> weak_ptr_t;

    tasklet(const io_service_ptr_t & io_srv)
     : tasklet_base(io_srv)
    { }

protected:

    tasklet_base::ptr_t shared_tasklet_base_from_this()
    { return this->shared_from_this(); }
};

    /*
        Examples of tasklets:
          - asio ioservice thread 'work' guards
          - listening sockets
          - peer discovery / polling
          


    */
}
}

#endif

