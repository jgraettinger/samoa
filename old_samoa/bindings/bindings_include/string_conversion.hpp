#ifndef BINDINGS_INCLUDE_STRING_CONVERSION_HPP
#define BINDINGS_INCLUDE_STRING_CONVERSION_HPP

#include "bindings_include/conversion_common.hpp"
#include <boost/python.hpp>

template<typename String>
struct string_to_python_converter
{
    static PyObject * convert(const String & s)
    {
        // Allocate a new PyString
        return boost::python::incref(
            boost::python::str(s.c_str(), s.size()).ptr()
        );
    }
    
    static void insert()
    {
        boost::python::to_python_converter<
            String,
            string_to_python_converter
        >();
    }
};

#endif // #ifndef

