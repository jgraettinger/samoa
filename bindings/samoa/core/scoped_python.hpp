#ifndef SAMOA_CORE_SCOPED_PYTHON_HPP
#define SAMOA_CORE_SCOPED_PYTHON_HPP

#include <exception>
#include <boost/python.hpp>
#include <boost/detail/atomic_count.hpp>
#include <Python.h>

namespace samoa {
namespace core {

// Python thread which entered proactor.run()
extern PyThreadState * _run_thread;

// Atomic reentrance-count of scoped_python struct
extern boost::detail::atomic_count _scoped_python_count;

inline void assert_in_run_thread()
{
    if(PyThreadState_Get() != _run_thread)
    {
        throw std::runtime_error("This operation must be invoked "
            "from the thread which invoked Proactor.run()");
    }
}

inline void exiting_python()
{
    assert_in_run_thread();

    // Release the GIL & persist run-thread context
    PyEval_SaveThread();
}

inline void entering_python()
{
    // Obtain the GIL & restore run-thread context
    PyEval_RestoreThread(_run_thread);
}

struct scoped_python
{
public:

    scoped_python()
    {
        if((++_scoped_python_count) == 1)
            entering_python();
    }

    ~scoped_python()
    {
        if((--_scoped_python_count) == 0)
            exiting_python();
    }
};

}
}

#endif
