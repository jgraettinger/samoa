#ifndef PYSAMOA_ITERUTIL_HPP
#define PYSAMOA_ITERUTIL_HPP

#include <boost/python.hpp>

namespace pysamoa {

/*
*  Convience methods for iterating over python sequences.
*  A boost::python object 'obj' can be iterated over as:
*
*  for(bpl::object it = iter(obj), item; next(it, item);)
*  { }
*/

inline boost::python::object iter(boost::python::object o)
{
    // if dict, call o.iteritems()
    if(boost::python::extract<boost::python::dict>(o).check())
        return boost::python::extract<boost::python::dict>(o)().iteritems();
    
    // otherwise, call iter(o) {returns new reference}
    return boost::python::object(
        boost::python::handle<>(PyObject_GetIter(o.ptr())));
}

inline bool next(const boost::python::object & iter, boost::python::object & o)
{
    // returns new reference, or null
    PyObject * py_ptr = PyIter_Next(iter.ptr());

    if(!py_ptr)
    {
        if(PyErr_Occurred())
        {
            boost::python::throw_error_already_set();
        }
        return false;
    }

    o = boost::python::object(boost::python::handle<>(py_ptr));
    return true;
}

}

#endif
