#include "samoa/core/proactor.hpp"
#include "pysamoa/scoped_python.hpp"
#include "pysamoa/coroutine.hpp"
#include <boost/python.hpp>
#include <boost/python/operators.hpp>
#include <Python.h>
#include <iostream>

namespace samoa {
namespace core {

namespace bpl = boost::python;
using namespace std;

void on_py_run_later(const bpl::object & callable,
    const bpl::tuple & args, const bpl::dict & kwargs)
{
    pysamoa::scoped_python block;

    // invoke callable
    bpl::object result = callable(*args, **kwargs);

    if(PyGen_Check(result.ptr()))
    {
        // result is a generator => start a new coroutine
        pysamoa::coroutine::ptr_t coro(new pysamoa::coroutine(result));
        coro->next();
    }
    else if(result.ptr() != Py_None)
    {
        string s_res = bpl::extract<string>(bpl::str(result));
        string s_callable = bpl::extract<string>(bpl::str(callable));
        string s_args = bpl::extract<string>(bpl::str(args));
        string s_kwargs = bpl::extract<string>(bpl::str(kwargs));

        throw runtime_error("Proactor.run_later(): expected "
            "callable to return either a generator or None, "
            "but got:\n\t" + s_res + "\n<callable was " + s_callable + ">\n"
            "<args were " + s_args + ">\n"
            "<kwargs were " + s_kwargs + ">\n");
    }
}

void py_run_later(proactor & p, const bpl::object & callable,
    unsigned delay_ms, const bpl::tuple & args, const bpl::dict & kwargs)
{
    p.run_later(boost::bind(&on_py_run_later,
        callable, args, kwargs), delay_ms);
}

void py_spawn(proactor & p, const bpl::object & callable,
    const bpl::tuple & args, const bpl::dict & kwargs)
{
    p.run_later(boost::bind(&on_py_run_later, callable, args, kwargs), 0);
}

void py_shutdown(proactor & p)
{
    p.get_nonblocking_io_service().stop();
}

void py_run(proactor & p)
{
    pysamoa::set_run_thread guard;

    p.get_nonblocking_io_service().run();
    // clean exit => no more work
    p.get_nonblocking_io_service().reset();
}

void make_proactor_bindings()
{
    bpl::class_<proactor, proactor::ptr_t, boost::noncopyable>(
        "Proactor", bpl::init<>())
        .def("run", &py_run)
        .def("run_later", &py_run_later, (
            bpl::arg("callable"),
            bpl::arg("delay_ms") = 0,
            bpl::arg("args") = bpl::tuple(),
            bpl::arg("kwargs") = bpl::dict()))
        .def("spawn", &py_spawn, (
            bpl::arg("callable"),
            bpl::arg("args") = bpl::tuple(),
            bpl::arg("kwargs") = bpl::dict()))
        .def("shutdown", &py_shutdown);
}

}
}

