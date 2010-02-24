#ifndef BINDINGS_INCLUDE_PAIR_CONVERSION_HPP
#define BINDINGS_INCLUDE_PAIR_CONVERSION_HPP

#include <boost/python.hpp>

template<typename Pair>
struct pair_to_python_converter
{
    static PyObject * convert(const Pair & p)
    {
        return boost::python::incref(
            boost::python::make_tuple(p.first, p.second).ptr()
        );
    }

    static void insert()
    {
        boost::python::to_python_converter<
            Pair,
            pair_to_python_converter
        >();
    }
};

template<typename Pair>
struct pair_from_python_converter
{
    static void * convertible(PyObject * o)
    {
        if(!PySequence_Check(o))
            return 0;
        
        Py_ssize_t len = PySequence_Length(o);
        if(len != 2)
            return 0;
        
        bool ok;
        PyObject * item = PySequence_ITEM(o, 0);
        ok = boost::python::extract<typename Pair::first_type>(item).check();
        Py_DECREF(item);
        if(!ok) return 0;
        
        item = PySequence_ITEM(o, 1);
        ok = boost::python::extract<typename Pair::second_type>(item).check();
        Py_DECREF(item);
        if(!ok) return 0;
        
        return o;
    }
    
    static void construct(
        PyObject * o,
        boost::python::converter::rvalue_from_python_stage1_data * data
    )
    {
        // Magic incantation to get at boost::python allocated storage
        void * bytes = reinterpret_cast<
            boost::python::converter::rvalue_from_python_storage<Pair>*
        >(data)->storage.bytes;
        
        PyObject * first = PySequence_ITEM(o, 0);
        PyObject * second = PySequence_ITEM(o, 1);
        
        Pair * pair = new (bytes) Pair(
            boost::python::extract<typename Pair::first_type>(first)(),
            boost::python::extract<typename Pair::second_type>(second)()
        );
        // Postcondition requires convertible point to constructed obj 
        data->convertible = pair;
        
        Py_DECREF(first);
        Py_DECREF(second);
        return;
    }
    
    static void insert()
    {   
        boost::python::converter::registry::push_back(
            &convertible,
            &construct,
            boost::python::type_id<Pair>()
        );
    }

};

#endif // guard
