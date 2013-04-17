#ifndef SAMOA_SPINLOCK_HPP
#define SAMOA_SPINLOCK_HPP

#include <atomic>

namespace samoa {

// Simple RAII spinlock wrapper around an atomic bool.
class spinlock
{
public:

    void acquire()
    {
		while(_lock.exchange(true)) { }
    }

    void release()
    {
		_lock = false;
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
        spinlock& _sl;
    };

private:

    friend class guard;
    std::atomic<bool> _lock = {false};
};

}

#endif

