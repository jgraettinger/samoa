#ifndef PYSAMOA_SCOPED_PYTHON_HPP
#define PYSAMOA_SCOPED_PYTHON_HPP

#include <exception>
#include <boost/python.hpp>
#include <boost/detail/atomic_count.hpp>
#include <Python.h>

namespace pysamoa {

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

class scoped_python
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

class set_run_thread
{
public:

    set_run_thread()
    {
        if(_run_thread)
        {
            throw std::runtime_error("set_run_thread(): Another python thread "
                "is already acting as the run thread");
        }

        // Release the GIL, and save this thread state. Future calls
        //  in to python from proactor handlers will call from this
        //  thread context.
        _run_thread = PyEval_SaveThread();
    }

    ~set_run_thread()
    {
        // Obtain the GIL & restore this thread state. Clear the
        //  _run_thread variable so that this or other threads
        //  may invoke Proactor.run()
        PyEval_RestoreThread(_run_thread);
        _run_thread = 0;
    }
};

} // end pysamoa

#endif
