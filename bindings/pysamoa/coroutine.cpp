#include "coroutine.hpp"
#include "future.hpp"
#include "scoped_python.hpp"
#include <boost/python.hpp>
#include <iostream>

namespace pysamoa {
namespace bpl = boost::python;
using namespace std;

// Precondition: Python GIL is held (Py_INCREF called under the hood)
coroutine::coroutine(const bpl::object & generator)
 : _exception_set(false),
   _stack(generator, 1)
{
    std::cerr << "coro " << (size_t)this << " created" << std::endl;
}

coroutine::~coroutine()
{
    scoped_python block;

    _stack.clear();

    std::cerr << "coro " << (size_t)this << " destroyed" << std::endl;
}

void coroutine::next()
{
    send(bpl::object());
}

void coroutine::send(const bpl::object & arg)
{
    reenter(arg);
}

void coroutine::error(const bpl::object & exc_type, const bpl::object & exc_msg)
{
    _exc_type = exc_type;
    _exc_val = exc_msg;
    _exception_set = true;

    reenter(bpl::object());
}

void coroutine::reenter(bpl::object arg)
{
    if(_stack.empty())
    {
        throw runtime_error("coroutine.reenter(): attempt to reenter"
            "a coroutine that has already exited");
    }

    while(true)
    {
        try
        {
            bpl::object frame = _stack.back();
            bpl::object result;

            if(_exception_set)
            {
                result = frame.attr("throw")(
                    _exc_type, _exc_val, _exc_traceback);

                // clear exception state 
                _exc_type = _exc_val = _exc_type = bpl::object();
                _exception_set = false;
            }
            else
            {
                result = frame.attr("send")(arg);
            }

            /*
            Possible values of result:

            A new generator (function call):
                - generator is pushed onto stack
                - arg is set to None

            A future (blocking wait):
                - set yielding coroutine on future & return

            Any other value (return):
                - if len(stack) > 1, save into frame_results
                - otherwise, it's an error

            An exception:
                - if StopIteration:
                    - clear error
                    - pop current stack frame
                    - set arg to frame return value
                - else:
                    - unwind stack by one
                    - if no more stack, rethrow, rethrow
                    - fetch exception / clear error
                    - send via throw to parent stack frame
            */

            if(PyGen_Check(result.ptr()))
            {
                // result is a generator to be called into
                _stack.push_back(result);
                arg = bpl::object();
            }
            else if(bpl::extract<future &>(result).check())
            {
                // result is a future to be waited upon
                future & new_future = bpl::extract<future &>(result)();
                new_future.set_yielding_coroutine(shared_from_this());
                return;
            }
            else if(_stack.size() != 1)
            {
                // result is a value to be returned to parent frame
                _frame_return.push_back(result);
            }
            else
            {
                // result is a value, but this is the bottom-most frame
                string msg = bpl::extract<string>(result.attr("__repr__")());
                string f_msg = bpl::extract<string>(frame.attr("__repr__")());
                throw runtime_error("coroutine.send(): expected "
                    "coroutine to yield a generator or a future, "
                    "but got:\n\t" + msg + "\n<frame was " + f_msg + ">");
            }
        }
        catch(bpl::error_already_set)
        {
            if(PyErr_ExceptionMatches(PyExc_StopIteration))
            {
                cout << "caught stop_iteration" << endl;

                // This is a return from the current frame
                PyErr_Clear();
                _stack.pop_back();

                if(_stack.empty())
                {
                    // end of coroutine
                    return;
                }

                // Wrangle return value
                if(_frame_return.empty())
                {
                    // No values yielded => return None
                    arg = bpl::object();
                }
                else if(_frame_return.size() == 1)
                {
                    // One value yielded => return value
                    arg = _frame_return[0];
                }
                else
                {
                    // Multiple values yielded => return a tuple
                    arg = bpl::tuple(_frame_return);
                }
                _frame_return.clear();
            }
            else
            {
                cout << "caught non-stop_iteration" << endl;

                // Exceptional case
                // Big Fat Note: This exception may have been thrown
                //  by the current stack frame. OR, we may be re-catching
                //  a re-thrown exception from a re-entrent call to
                //  reenter(). Thus, we need to check that the stack hasn't
                //  already been fully unwound before attempting to unwind
                //  the throwing current frame.

                if(!_stack.empty())
                    _stack.pop_back();

                // No remaining stack to potentially
                //   handle the exception: re-raise
                if(_stack.empty())
                { throw; }

                cout << "still stack to unwind: reraising in python" << endl;
                // Fetch & clear error from python interpreter
                PyObject * ptype, * pval, * ptrace;
                PyErr_Fetch(&ptype, &pval, &ptrace);

                // Instantiate exception instance (if it isn't already)
                PyErr_NormalizeException(&ptype, &pval, &ptrace);

                // PyErr_Fetch returned new references. Pass reference
                //   ownership to holding boost::python::objects
                _exc_type = bpl::object(bpl::handle<>(ptype));
                _exc_val  = bpl::object(bpl::handle<>(bpl::allow_null(pval)));
                _exc_traceback = bpl::object(
                    bpl::handle<>(bpl::allow_null(ptrace)));

                _exception_set = true;
            }
        }
    } // end while true
}

}

