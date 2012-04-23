#include <boost/python.hpp>
#include "samoa/core/fwd.hpp"
#include "samoa/core/proactor.hpp"
#include "pysamoa/scoped_python.hpp"
#include "pysamoa/coroutine.hpp"
#include "pysamoa/future.hpp"
#include "pysamoa/iterutil.hpp"
#include <boost/python/operators.hpp>
#include <memory>

namespace samoa {
namespace core {

namespace bpl = boost::python;
using namespace std;

//////////////////////////////////////////////////////////
// proactor.run_later() support

void py_run_later(proactor & p, const bpl::object & gen, unsigned delay_ms)
{
    pysamoa::coroutine::ptr_t coro = boost::make_shared<pysamoa::coroutine>(
        gen, p.serial_io_service());

    auto on_later = [coro]()
    {
        pysamoa::python_scoped_lock block;

        coro->next();
    };
    p.run_later(on_later, delay_ms);
}

//////////////////////////////////////////////////////////
// proactor.wait_until_idle() support

struct idle_timeout {
    pysamoa::future::ptr_t future;
};

pysamoa::future::ptr_t py_wait_until_idle(proactor & p)
{
    pysamoa::future::ptr_t f = boost::make_shared<pysamoa::future>();

    auto throw_closure = [f]()
    {
        throw idle_timeout({f});
    };
    p.run_later(throw_closure, 5);
    return f;
}

//////////////////////////////////////////////////////////
// proactor.run() support

void py_run(proactor & p, bpl::object generator)
{
    SAMOA_ASSERT(p.get_declared_io_service() && \
        "serial or concurrent io-service must be declared from this thread");

    if(generator)
    {
        pysamoa::future::ptr_t f = py_wait_until_idle(p);
        f->set_yielding_coroutine(boost::make_shared<pysamoa::coroutine>(
            generator, p.serial_io_service()));
    }

    // release GIL - must be re-aquired before manipulating python objects
    pysamoa::python_scoped_unlock unlock;

    boost::asio::io_service & io_srv = *p.get_declared_io_service();

    bool was_idle = true;

    while(true)
    {
        try
        {
            if(was_idle)
            {
                if(!io_srv.run_one())
                {
                    // exit condition: no further work
                    break;
                }
                was_idle = false;
            }
            else
            {
                io_srv.run();
                // exit condition: no further work
                break;
            }
        }
        catch(idle_timeout to)
        {
            pysamoa::python_scoped_lock lock;

            if(io_srv.stopped())
            {
                // if idle timer was the only current handler,
                //  io_service stopped flag is set prior to throw
                io_srv.reset();
            }

            if(!was_idle)
            {
                // we completed some handlers; just restart the timer
                pysamoa::future::ptr_t f = py_wait_until_idle(p);
                f->set_yielding_coroutine(to.future->yielding_coroutine());

                was_idle = true;
            }
            else
            {
                // we were idle; call into future
                SAMOA_ASSERT(to.future->is_yielded());
                to.future->on_result(bpl::object());
            }
        }
    }
    io_srv.reset();
}


void make_proactor_bindings()
{
    bpl::class_<proactor, proactor_ptr_t, boost::noncopyable>(
            "Proactor", bpl::no_init)
        .def("get_proactor", &proactor::get_proactor)
        .staticmethod("get_proactor")
        .def("run_later", &py_run_later, (
            bpl::arg("generator"),
            bpl::arg("delay_ms") = 0))
        .def("run", &py_run)
        .def("wait_until_idle", &py_wait_until_idle)
        .def("shutdown", &proactor::shutdown)
        .def("serial_io_service", &proactor::serial_io_service)
        .def("concurrent_io_service", &proactor::concurrent_io_service)
        .def("declare_serial_io_service",
            &proactor::declare_serial_io_service)
        .def("declare_concurrent_io_service",
            &proactor::declare_concurrent_io_service)
        ;

    bpl::class_<boost::asio::io_service, io_service_ptr_t, boost::noncopyable>(
        "_ioservice", bpl::no_init);

}

}
}

