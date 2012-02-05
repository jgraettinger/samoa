#include <boost/python.hpp>
#include "samoa/core/fwd.hpp"
#include "samoa/core/proactor.hpp"
#include "pysamoa/scoped_python.hpp"
#include "pysamoa/coroutine.hpp"
#include "pysamoa/future.hpp"
#include "pysamoa/iterutil.hpp"
#include <boost/python/operators.hpp>
#include <boost/bind.hpp>
#include <memory>

namespace samoa {
namespace core {

namespace bpl = boost::python;
using namespace std;

//////////////////////////////////////////////////////////
// proactor.run_later() support

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

//////////////////////////////////////////////////////////
// proactor.run_test() support

struct idle_timeout {};

void on_idle(const io_service_ptr_t &)
{
    throw idle_timeout();
}

void py_run_test(proactor & p, bpl::object test_steps,
    unsigned idle_timeout_ms)
{
    SAMOA_ASSERT(p.get_declared_io_service() && \
        "serial or concurrent io-service must be declared from this thread");

    bpl::object next_step, step_iter;

    if(PyDict_Check(test_steps.ptr()) || \
       PyObject_HasAttrString(test_steps.ptr(), "__iter__"))
    {
        step_iter = pysamoa::iter(test_steps);
        SAMOA_ASSERT(pysamoa::next(step_iter, next_step) && "no test steps");
    }
    else
    {
        SAMOA_ASSERT(PyCallable_Check(test_steps.ptr()));
        next_step = test_steps;
    }

    // release GIL - must be re-aquired before manipulating python objects
    pysamoa::python_scoped_unlock unlock;

    boost::asio::io_service & io_srv = *p.get_declared_io_service();
    std::unique_ptr<boost::asio::io_service::work> work(
        new boost::asio::io_service::work(io_srv));

    // initialize idle timer
    timer_ptr_t idle_timer = p.run_later(on_idle, idle_timeout_ms);
    bool was_idle = true;

    while(true)
    {
        try
        {
            if(!io_srv.run_one())
            {
                // exit condition: no further work
                io_srv.reset();
                break;
            }
            was_idle = false;
        }
        catch(idle_timeout)
        {
            if(!was_idle)
            {
                // we completed some handlers; just restart the timer
                idle_timer = p.run_later(on_idle, idle_timeout_ms);
                was_idle = true;
            }
            else
            {
                // aquire python GIL
                pysamoa::python_scoped_lock lock;

                // submit next test step
                py_run_later(p, next_step, 0, bpl::tuple(), bpl::dict());

                if(step_iter && pysamoa::next(step_iter, next_step))
                {
                    // still more steps to be done; restart idle timer
                    idle_timer = p.run_later(on_idle, idle_timeout_ms);
                    was_idle = true;
                }
                else
                {
                    work.reset();
                }
            }
        }
    }
}

//////////////////////////////////////////////////////////
// proactor.run() support

void py_run(proactor & p)
{
    SAMOA_ASSERT(p.get_declared_io_service() && \
        "serial or concurrent io-service must be declared from this thread");

    // release GIL - must be re-aquired before manipulating python objects
    pysamoa::python_scoped_unlock unlock;

    boost::asio::io_service & io_srv = *p.get_declared_io_service();
    std::unique_ptr<boost::asio::io_service::work> work(
        new boost::asio::io_service::work(io_srv));

    io_srv.run(); 
}


void make_proactor_bindings()
{
    bpl::class_<proactor, proactor_ptr_t, boost::noncopyable>(
            "Proactor", bpl::no_init)
        .def("get_proactor", &proactor::get_proactor)
        .staticmethod("get_proactor")
        .def("run_test", &py_run_test, (
            bpl::arg("test_steps"), bpl::arg("idle_timeout_ms") = 5))
        .def("run_later", &py_run_later, (
            bpl::arg("callable"),
            bpl::arg("delay_ms") = 0,
            bpl::arg("args") = bpl::tuple(),
            bpl::arg("kwargs") = bpl::dict()))
        .def("run", &py_run)
        .def("shutdown", &proactor::shutdown)
        .def("serial_io_service", &proactor::serial_io_service)
        .def("concurrent_io_service", &proactor::concurrent_io_service)
        .def("declare_serial_io_service",
            &proactor::declare_serial_io_service)
        .def("declare_concurrent_io_service",
            &proactor::declare_concurrent_io_service);

    bpl::class_<boost::asio::io_service, io_service_ptr_t, boost::noncopyable>(
        "_ioservice", bpl::no_init);

}

}
}

