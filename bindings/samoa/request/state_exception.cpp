#include "pysamoa/boost_python.hpp"
#include "samoa/request/state_exception.hpp"
#include "samoa/error.hpp"

namespace samoa {
namespace request {

namespace bpl = boost::python;

bpl::object py_klass;

void set_state_exception_python_class(bpl::object klass)
{
    py_klass = klass;
}

void translate_func(const state_exception & e)
{
    SAMOA_ASSERT(py_klass);

    bpl::object py_e = py_klass(e.get_code(), e.what()); 
    PyErr_SetObject(py_klass.ptr(), py_e.ptr());
}

void make_state_exception_bindings()
{
    bpl::def("_set_state_exception_python_class",
        &set_state_exception_python_class);

    bpl::register_exception_translator<state_exception>(&translate_func);
}

}
}
