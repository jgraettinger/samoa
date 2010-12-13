#include "samoa/core/proactor.hpp"
#include "samoa/core/runthread.hpp"
#include <boost/python.hpp>
#include <Python.h>
#include <iostream>

namespace samoa {
namespace core {

using namespace boost::python;

void py_run(proactor & p)
{
    if(_run_thread)
    {
        throw std::runtime_error("Another python thread has "
            "already invoked Proactor.run()");
    }

    // Release the GIL, and save this thread state. Future calls
    //  in to python from proactor handlers will call from this
    //  thread context.
    _run_thread = PyEval_SaveThread();

    bool running = true;
    while(running)
    {
        try
        {
            p.get_nonblocking_io_service().run();
            // clean exit => no more work
            running = false;
            p.get_nonblocking_io_service().reset();
        }
        catch(error_already_set) {

            // restore python thread, so we can read exception state
            scoped_python block;

            if(PyErr_ExceptionMatches(PyExc_KeyboardInterrupt))
                running = false;

            std::cerr << "<Caught by Proactor.run()>:" << std::endl;
            PyErr_PrintEx(0);
        }
        catch(const std::exception & e)
        {
            std::cerr << "Caught: " << e.what() << std::endl;
        }
    }

    // Obtain the GIL & restore this thread state. Clear the
    //  _run_thread variable so that this or other threads
    //  may invoke Proactor.run()
    PyEval_RestoreThread(_run_thread);
    _run_thread = 0;
}

void make_proactor_bindings()
{
    class_<proactor, proactor::ptr_t, boost::noncopyable>(
        "Proactor", init<>())
        .def("run", &py_run);
}

}
}

