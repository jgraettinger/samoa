#ifndef PYSAMOA_SCOPED_PYTHON_HPP
#define PYSAMOA_SCOPED_PYTHON_HPP

#include <Python.h>
#include "samoa/error.hpp"
#include "pysamoa/boost_python.hpp"
#include <boost/thread/tss.hpp>

namespace pysamoa {

// Stored python thread corresponding to this native thread
extern boost::thread_specific_ptr<PyThreadState> _saved_python_thread;

class python_scoped_lock
{
public:

    python_scoped_lock()
     : _guards_gil(_saved_python_thread.get() != 0)
    {
        if(_guards_gil)
        {
            // aqquire GIL, clear python thread
            PyEval_RestoreThread(_saved_python_thread.get());
            _saved_python_thread.reset();
        }
    }

    ~python_scoped_lock()
    {
        if(_guards_gil)
        {
            // release GIL, save python thread
            _saved_python_thread.reset(PyEval_SaveThread());
        }
    }

private:

    bool _guards_gil;
};

class python_scoped_unlock
{
public:

    python_scoped_unlock()
    {
        SAMOA_ASSERT(!_saved_python_thread.get());

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
