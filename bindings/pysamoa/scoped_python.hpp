#ifndef PYSAMOA_SCOPED_PYTHON_HPP
#define PYSAMOA_SCOPED_PYTHON_HPP

#include <exception>
#include <boost/python.hpp>
#include <boost/detail/atomic_count.hpp>
#include <boost/thread.hpp>
#include <Python.h>

namespace pysamoa {

// Stored python thread corresponding to this native thread
extern boost::thread_specific_ptr<PyThreadState> _saved_python_thread;

class python_scoped_lock
{
public:

    python_scoped_lock()
    {
        if(_saved_python_thread.get())
        {
            // aqquire GIL, clear python thread
            PyEval_RestoreThread(_saved_python_thread.get());
            _saved_python_thread.reset();

            assert(_reentrance_count == 0);
        }

        ++_reentrance_count;
    }

    ~python_scoped_lock()
    {
        if((--_reentrance_count) == 0)
        {
            // release GIL, save python thread
            _saved_python_thread.reset(PyEval_SaveThread());
        }
    }

private:

    // note this variable is shared accross threads. However, it's
    //   only updated when the GIL is held, and thus synchronized.
    static unsigned _reentrance_count;
};

class python_scoped_unlock
{
public:

    python_scoped_unlock()
    {
        if(_saved_python_thread.get())
        {
            throw std::runtime_error("python_scoped_unlock(): "
                "re-entrant calls are not allowed");
        }

        // Release the GIL, and save this thread state. Future calls
        //  in to python from proactor handlers will call from this
        //  thread context.
        _saved_python_thread.reset(PyEval_SaveThread());
    }

    ~python_scoped_unlock()
    {
        // Obtain the GIL & restore this thread state. Clear the
        //  _run_thread variable so that this or other threads
        //  may invoke Proactor.run()
        PyEval_RestoreThread(_saved_python_thread.get());
        _saved_python_thread.reset();
    }
};

} // end pysamoa

#endif
