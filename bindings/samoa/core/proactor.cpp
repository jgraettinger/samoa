#include <boost/python.hpp>
#include "samoa/core/fwd.hpp"
#include "samoa/core/proactor.hpp"
#include "pysamoa/scoped_python.hpp"
#include "pysamoa/coroutine.hpp"
#include <boost/python/operators.hpp>
#include <boost/bind.hpp>

#include <iostream>

namespace samoa {
namespace core {

namespace bpl = boost::python;
using namespace std;

void on_py_run_later(const io_service_ptr_t & io_srv,
    const bpl::object & callable,
    const bpl::tuple & args,
    const bpl::dict & kwargs)
{
    pysamoa::python_scoped_lock block;

    // invoke callable
    bpl::object result = callable(*args, **kwargs);

    if(PyGen_Check(result.ptr()))
    {
        // result is a generator => start a new coroutine
        pysamoa::coroutine::ptr_t coro(
            new pysamoa::coroutine(result, io_srv));
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
    p.run_later(boost::bind(&on_py_run_later, _1,
        callable, args, kwargs), delay_ms);
}

void py_spawn(proactor & p, const bpl::object & callable,
    const bpl::tuple & args, const bpl::dict & kwargs)
{
    p.run_later(boost::bind(&on_py_run_later, _1,
        callable, args, kwargs), 0);
}

void py_run(proactor & p, bool exit_when_idle)
{
    pysamoa::python_scoped_unlock unlock;

    p.run(exit_when_idle);
}

void make_proactor_bindings()
{
    bpl::class_<proactor, proactor_ptr_t, boost::noncopyable>(
        "Proactor", bpl::init<>())
        .def("run", &py_run, (
            bpl::arg("exit_when_idle") = true))
        .def("run_later", &py_run_later, (
            bpl::arg("callable"),
            bpl::arg("delay_ms") = 0,
            bpl::arg("args") = bpl::tuple(),
            bpl::arg("kwargs") = bpl::dict()))
        .def("spawn", &py_spawn, (
            bpl::arg("callable"),
            bpl::arg("args") = bpl::tuple(),
            bpl::arg("kwargs") = bpl::dict()))
        .def("shutdown", &proactor::shutdown)
        .def("serial_io_service", &proactor::serial_io_service)
        .def("concurrent_io_service", &proactor::concurrent_io_service);

    bpl::class_<boost::asio::io_service, io_service_ptr_t, boost::noncopyable>(
        "_ioservice", bpl::no_init);

}

}
}

