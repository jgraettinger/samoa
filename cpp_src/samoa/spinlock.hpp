#ifndef SAMOA_SPINLOCK_HPP
#define SAMOA__SPINLOCK_HPP

#include "samoa/error.hpp"
#include <pthread.h>

namespace samoa {

// Lite RAII wrapper around pthread's spinlock
class spinlock
{
public:

    spinlock()
    {
        checked_throw(pthread_spin_init(&_lock, PTHREAD_PROCESS_PRIVATE),
            "spinlock::spinlock(): pthread_spin_init");
    }

    ~spinlock()
    {
        checked_abort(pthread_spin_destroy(&_lock),
            "spinlock::~spinlock(): pthread_spin_destroy");
    }

    class guard
    {
    public:

        guard(spinlock & sl)
         : _plock(&sl._lock)
        {
            checked_throw(pthread_spin_lock(_plock),
                "spinlock::guard::guard(): pthread_spin_lock");
        }

        ~guard()
        {
            checked_abort(pthread_spin_unlock(_plock),
                "spinlock::guard::~guard(): pthread_spin_unlock");
        }

    private:

        pthread_spinlock_t * _plock;
    };

private:

    friend class guard;

    pthread_spinlock_t _lock;
};

}

#endif

