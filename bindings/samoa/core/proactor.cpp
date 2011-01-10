#include "samoa/core/proactor.hpp"
#include "pysamoa/scoped_python.hpp"
#include "pysamoa/coroutine.hpp"
#include <boost/python.hpp>
#include <Python.h>
#include <iostream>

namespace samoa {
namespace core {

using namespace boost::python;
using namespace std;

void on_py_run_later(const boost::system::error_code & ec, object target)
{
    if(ec)
    {
        cerr << "on_py_run_later(): " << ec.message() << endl;
        return;
    }

    pysamoa::scoped_python block;

    // invoke target
    object result = target();

    if(PyGen_Check(result.ptr()))
    {
        // start a new coroutine
        pysamoa::coroutine::ptr_t coro(new pysamoa::coroutine(result));
        coro->next();
    }
    else if(result.ptr() != Py_None)
    {
        string msg = extract<string>(result.attr("__repr__")());
        string t_msg = extract<string>(target.attr("__repr__")());
        throw runtime_error("Proactor.run_later(): expected "
            "target to return either a generator or None, "
            "but got:\n\t" + msg + "\n<target was " + t_msg + ">");
    }
}

void py_run_later(proactor & p, object target, unsigned delay_ms)
{
    if(delay_ms)
    {
        boost::asio::deadline_timer t(p.get_nonblocking_io_service(),
            boost::posix_time::milliseconds(delay_ms));

        t.async_wait(boost::bind(&on_py_run_later,
            _1, target));
    }
    else
    {
        // post to proactor io_service for immediate dispatch
        p.get_nonblocking_io_service().post(boost::bind(&on_py_run_later,
            boost::system::error_code(), target));
    }
}

void py_shutdown(proactor & p)
{
    PyErr_SetNone(PyExc_KeyboardInterrupt);
    throw_error_already_set();
}

void py_run(proactor & p)
{
    if(pysamoa::_run_thread)
    {
        throw runtime_error("Another python thread has "
            "already invoked Proactor.run()");
    }

    // Release the GIL, and save this thread state. Future calls
    //  in to python from proactor handlers will call from this
    //  thread context.
    pysamoa::_run_thread = PyEval_SaveThread();

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
            pysamoa::scoped_python block;

            if(PyErr_ExceptionMatches(PyExc_KeyboardInterrupt))
            {
                running = false;
                cerr << "Shutting down..." << endl;
                PyErr_Clear();
            }
            else {
                cerr << "<Caught by Proactor.run()>:" << endl;
                PyErr_PrintEx(0);
            }
        }
        catch(const exception & e)
        {
            cerr << "Caught: " << e.what() << endl;
        }
    }

    // Obtain the GIL & restore this thread state. Clear the
    //  pysamoa::_run_thread variable so that this or other threads
    //  may invoke Proactor.run()
    PyEval_RestoreThread(pysamoa::_run_thread);
    pysamoa::_run_thread = 0;
}

void make_proactor_bindings()
{
    class_<proactor, proactor::ptr_t, boost::noncopyable>(
        "Proactor", init<>())
        .def("run", &py_run)
        .def("run_later", &py_run_later, (
            boost::python::arg("target"), boost::python::arg("delay_ms") = 0))
        .def("shutdown", &py_shutdown);
}

}
}

