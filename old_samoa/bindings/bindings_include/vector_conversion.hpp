#ifndef BINDINGS_INCLUDE_VECTOR_CONVERSION_HPP
#define BINDINGS_INCLUDE_VECTOR_CONVERSION_HPP

#include "bindings_include/conversion_common.hpp"
#include <boost/python.hpp>

template<typename Vector>
struct vector_to_python_converter
{
    static PyObject * convert(const Vector & v)
    {
        // Allocate a new tuple of proper size
        boost::python::object tuple(
            (boost::python::detail::new_reference)
            PyTuple_New(v.size())
        );
        
        size_t pos = 0;
        for(typename Vector::const_iterator i = v.begin(); i != v.end(); ++i)
        {
            // Tuple steals reference => explictly increase ref count
            PyTuple_SET_ITEM(
                tuple.ptr(), pos++,
                boost::python::incref(
                    boost::python::object(*i).ptr()
                )
            );
        }
        return boost::python::incref(tuple.ptr());
    }
    
    static void insert()
    {
        boost::python::to_python_converter<
            Vector,
            vector_to_python_converter
        >();
    }
};

template<typename Vector>
struct vector_from_python_converter
{
    static void fast_sequence_construct(
        PyObject * obj,
        boost::python::converter::rvalue_from_python_stage1_data * data
    )
    {
        // Magic incantation to get at boost::python allocated storage
        void * bytes = reinterpret_cast<
            boost::python::converter::rvalue_from_python_storage<Vector>*
        >(data)->storage.bytes;
        
        // Recover fast-sequence obtained by fast_sequence_convertible
        PyObject * fast_seq = (PyObject *) data->convertible;
        
        Vector * vec = new (bytes) Vector();
        // Postcondition requires convertible point to constructed obj 
        data->convertible = vec;
        
        Py_ssize_t len = PySequence_Fast_GET_SIZE(fast_seq);
        PyObject ** items = PySequence_Fast_ITEMS(fast_seq);
        
        vec->reserve( len);
        
        for(Py_ssize_t i = 0; i != len; ++i)
        {
            vec->push_back( boost::python::extract<
                typename Vector::value_type>(items[i])()
            );
        }
        Py_DECREF(fast_seq);
        return;
    }
    
    static void insert()
    {   
        boost::python::converter::registry::push_back(
            &fast_sequence_convertible<typename Vector::value_type>,
            &fast_sequence_construct,
            boost::python::type_id<Vector>()
        );
    }
};

#endif // guard

