#ifndef SAMOA_SPINLOCK_HPP
#define SAMOA_SPINLOCK_HPP

#include "samoa/error.hpp"
#include <pthread.h>

namespace samoa {

// Lite RAII wrapper around pthread's spinlock
class spinlock
{
public:

    spinlock()
    {
        SAMOA_ASSERT_ERRNO(pthread_spin_init(&_lock, PTHREAD_PROCESS_PRIVATE));
    }

    ~spinlock()
    {
        SAMOA_ABORT_ERRNO(pthread_spin_destroy(&_lock));
    }

    void acquire()
    {
        SAMOA_ASSERT_ERRNO(pthread_spin_lock(&_lock));
    }

    void release()
    {
        SAMOA_ABORT_ERRNO(pthread_spin_unlock(&_lock));
    }

    class guard
    {
    public:

        guard(spinlock & sl)
         : _sl(sl)
        {
            _sl.acquire();
        }

        ~guard()
        {
            _sl.release();
        }

    private:

        spinlock & _sl;
    };

private:

    friend class guard;

    pthread_spinlock_t _lock;
};

}

#endif

