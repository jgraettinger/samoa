#ifndef SAMOA_CORE_PYTHREAD_HPP
#define SAMOA_CORE_PYTHREAD_HPP

#include <exception>
#include <boost/python.hpp>
#include <Python.h>

namespace samoa {
namespace core {

extern PyThreadState * _run_thread;

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
    { entering_python(); }

    ~scoped_python()
    { exiting_python(); }
};

}
}

#endif
