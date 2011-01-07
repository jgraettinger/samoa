#include "coroutine.hpp"
#include "future.hpp"
#include "scoped_python.hpp"
#include <boost/python.hpp>
#include <iostream>

namespace pysamoa {
namespace bpl = boost::python;
using namespace samoa::core;

// Precondition: Python GIL is held (Py_INCREF called under the hood)
coroutine::coroutine(const bpl::object & generator)
 : _generator(generator)
{
    std::cerr << "coro " << (size_t)this << " created" << std::endl;
}

coroutine::~coroutine()
{
    scoped_python block;

    _generator = bpl::object();

    std::cerr << "coro " << (size_t)this << " destroyed" << std::endl;
}

void coroutine::start()
{
    send(bpl::object());
}

void coroutine::send(const bpl::object & arg)
{
    try
    {
        bpl::object res = _generator.attr("send")(arg);

        // coroutines may yield only futures
        //  if res is not a future, a python TypeError
        //    exception will be raised
        future & new_future = bpl::extract<future &>(res)();
        new_future.set_yielding_coroutine(shared_from_this());
    }
    catch(bpl::error_already_set)
    {
        if(!PyErr_ExceptionMatches(PyExc_StopIteration))
            throw;

        PyErr_Clear();
        std::cerr << "coroutine::send() got StopIteration" << std::endl;
    }
}

void coroutine::error(
    const bpl::object & exc_type, const bpl::object & exc_msg)
{
    try
    {
        bpl::object res = _generator.attr("throw")(exc_type, exc_msg);

        // coroutines may yield only futures
        //  if res is not a future, a python TypeError
        //    exception will be raised
        future & new_future = bpl::extract<future &>(res)();
        new_future.set_yielding_coroutine(shared_from_this());
    }
    catch(bpl::error_already_set)
    {
        if(!PyErr_ExceptionMatches(PyExc_StopIteration))
            throw;

        PyErr_Clear();
        std::cerr << "coroutine::error() got StopIteration" << std::endl;
    }
}

}

