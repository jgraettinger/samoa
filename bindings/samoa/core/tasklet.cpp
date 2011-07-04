
#include <boost/python.hpp>
#include "samoa/core/tasklet.hpp"
#include "samoa/core/tasklet_group.hpp"
#include "pysamoa/scoped_python.hpp"
#include "pysamoa/coroutine.hpp"

namespace samoa {
namespace core {

namespace bpl = boost::python;
using namespace std;


/*
It's crucial that, for Python tasklets deriving from py_tasklet, the final
reference to an instance is disposed of from python code (eg, within the
context of a pysamoa::python_scoped_lock block, where the python GIL is held).

A knotty issue when calling py_tasklet::run_tasklet via start_orphaned_tasklet
is that there may be no further python references to the instance, aside from
the shared_ptr used to make the call.

The machinery of tasklet_group is such that this shared_ptr will be destroyed
_outside_ of a pysamoa::python_scoped_lock context, and thus we may attempt
to illegally finalize & GC the python instance.

The solution implemented here is to 1) require that run_tasklet return a
coroutine, and 2) enter into that coroutine via post.

Since a python reference to the instance is wrapped up in the coroutine,
calling via post ensures that reference outlives the shared_ptr.
*/

class py_tasklet :
    public tasklet<py_tasklet>, public bpl::wrapper<py_tasklet>
{
public:

    using tasklet<py_tasklet>::ptr_t;

    py_tasklet(const io_service_ptr_t & io_srv)
     : tasklet<py_tasklet>(io_srv)
    { }

    void run_tasklet()
    {
        pysamoa::python_scoped_lock block;

        bpl::object method = this->get_override("run_tasklet");
        SAMOA_ASSERT(method);

        bpl::object result = method();

        // run_tasklet *must* return a generator; see note above
        //  about tasklet lifetime management
        SAMOA_ASSERT(PyGen_Check(result.ptr()));

        // start a new coroutine
        pysamoa::coroutine::ptr_t coro(new pysamoa::coroutine(
            result, get_io_service()));

        // _post_ to call into the corutine
        coro->next(true);
    }

    void halt_tasklet()
    {
        pysamoa::python_scoped_lock block;

        bpl::object method = this->get_override("halt_tasklet");
        SAMOA_ASSERT(method);

        bpl::object result = method();

        if(result.ptr() != Py_None)
        {
            string msg = bpl::extract<string>(result.attr("__repr__")());
            string h_msg = bpl::extract<string>(method.attr("__repr__")());
            throw runtime_error("tasklet.halt_tasklet(): expected "
                "method to return None, "
                "but got:\n\t" + msg + "\n<method was " + h_msg + ">");
        }
    }
};

void py_set_tasklet_name(py_tasklet & tlet, const bpl::str & py_str)
{
    char * buf = PyString_AS_STRING(py_str.ptr());

    tlet.set_tasklet_name(std::string(buf,
        buf + PyString_GET_SIZE(py_str.ptr())));
}

void make_tasklet_bindings()
{
    bpl::class_<tasklet_base, tasklet_base::ptr_t, boost::noncopyable>(
            "_TaskletBase", bpl::no_init)
        .def("get_io_service", &tasklet_base::get_io_service,
            bpl::return_value_policy<bpl::copy_const_reference>())
        .def("get_tasklet_group", &tasklet_base::get_tasklet_group,
            bpl::return_value_policy<bpl::copy_const_reference>())
        .def("get_tasklet_name", &tasklet_base::get_tasklet_name,
            bpl::return_value_policy<bpl::copy_const_reference>())
        ;

    bpl::class_<py_tasklet, py_tasklet::ptr_t,
        bpl::bases<tasklet_base>, boost::noncopyable>(
            "Tasklet", bpl::init<const io_service_ptr_t &>())
        .def("set_tasklet_name", &py_set_tasklet_name)
        ;
}

}
}
