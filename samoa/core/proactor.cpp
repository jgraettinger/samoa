#include "samoa/core/proactor.hpp"
#include <boost/python.hpp>
#include <Python.h>
#include <iostream>

namespace core {

using namespace boost::python;

void py_run(proactor & p)
{
    Py_BEGIN_ALLOW_THREADS;

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
        catch(const std::exception & e)
        {
            std::cerr << "Caught: " << e.what() << std::endl;
        }
    }

    Py_END_ALLOW_THREADS;
}

void make_proactor_bindings()
{
    class_<proactor, proactor::ptr_t, boost::noncopyable>(
        "proactor", init<>())
        .def("run", &py_run);
}

}

