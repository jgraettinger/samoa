#ifndef BINDINGS_INCLUDE_CONVERSION_COMMON_HPP
#define BINDINGS_INCLUDE_CONVERSION_COMMON_HPP

#include <boost/python.hpp>

template<typename ValueType>
static void * fast_sequence_convertible(PyObject * obj)
{
    if(!PySequence_Check(obj))
        return 0;
    
    // Convert to a 'fast' sequence (internal rep of list/tuple)
    PyObject * fast_seq = boost::python::expect_non_null(
        PySequence_Fast(
            obj, "sequence_convertible.PySequence_Fast() failed"
        )
    );
    
    Py_ssize_t len = PySequence_Fast_GET_SIZE(fast_seq);
    PyObject ** items = PySequence_Fast_ITEMS(fast_seq);
    
    for(Py_ssize_t i = 0; i != len; ++i)
    {
        if( !boost::python::extract<ValueType>(items[i]).check())
        {
            Py_DECREF(fast_seq);
            return 0;
        }
    }
    // Return fast_seq both to indicate success (as non-zero),
    //  and because we'll see in conversion as
    //  data->convertible (we can avoid rebuilding it)
    return fast_seq;
}

#endif // guard
