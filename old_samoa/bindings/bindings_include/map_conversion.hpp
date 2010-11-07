#ifndef BINDINGS_INCLUDE_MAP_CONVERSION_HPP
#define BINDINGS_INCLUDE_MAP_CONVERSION_HPP

#include "bindings_include/conversion_common.hpp"
#include <boost/python.hpp>

template<typename Map>
struct map_to_python_converter
{
    static PyObject * convert(const Map & m)
    {
        // Allocate a new dictionary
        boost::python::object dict(
            (boost::python::detail::new_reference)
            PyDict_New()
        );
        
        for(typename Map::const_iterator i = m.begin(); i != m.end(); ++i)
        {
            // SetItem will inc ref on key/value instances
            PyDict_SetItem(dict.ptr(),
                boost::python::object(i->first).ptr(),
                boost::python::object(i->second).ptr()
            );
        }
        
        return boost::python::incref(dict.ptr());
    }
    
    static void insert()
    {
        boost::python::to_python_converter<
            Map,
            map_to_python_converter
        >();
    }
};

template<typename Map>
struct map_from_python_converter
{
    static void * map_dict_convertible(PyObject * obj)
    {
        if(!PyDict_Check(obj))
            return 0;
        
        PyObject * key, * val;
        Py_ssize_t pos = 0;
        
        while(PyDict_Next(obj, &pos, &key, &val))
        {
            // PyDict_Next returns borrowed references
            if(!boost::python::extract<typename Map::key_type>(key).check())
                return 0;
            if(!boost::python::extract<typename Map::mapped_type>(val).check())
                return 0;
        }
        return;
    }
    
    static void map_convert_from_dict(
        PyObject * obj,
        boost::python::converter::rvalue_from_python_stage1_data * data
    )
    {
        // Magic incantation to get at boost::python allocated storage
        void * bytes = reinterpret_cast<
            boost::python::converter::rvalue_from_python_storage<Map>*
        >(data)->storage.bytes;
        
        Map * map = new (bytes) Map();
        // Postcondition requires convertible point to constructed obj 
        data->convertible = map;
        
        PyObject * key, * val;
        Py_ssize_t pos = 0;
        
        while(PyDict_Next(obj, &pos, &key, &val))
        {
            // PyDict_Next returns borrowed references
            (*map)[boost::python::extract<typename Map::key_type>(key)()] = \
                   boost::python::extract<typename Map::mapped_type>(val)();
        }
        return;
    }
    
    static void map_convert_from_fast_sequence(
        PyObject * o,
        boost::python::converter::rvalue_from_python_stage1_data * data
    )
    {
        // Magic incantation to get at boost::python allocated storage
        void * bytes = reinterpret_cast<
            boost::python::converter::rvalue_from_python_storage<Map>*
        >(data)->storage.bytes;
        
        // Recover fast-sequence obtained by fast_sequence_convertible
        PyObject * fast_seq = (PyObject *) data->convertible;
        
        Map * map = new (bytes) Map();
        // Postcondition requires convertible point to constructed obj 
        data->convertible = map;
        
        Py_ssize_t len = PySequence_Fast_GET_SIZE(fast_seq);
        PyObject ** items = PySequence_Fast_ITEMS(fast_seq);
        
        for(Py_ssize_t i = 0; i != len; ++i)
        {
            map->insert( boost::python::extract<
                typename Map::value_type>(items[i])());
        }
        Py_DECREF(fast_seq);
        return;
    }
    
    static void insert()
    {   
        boost::python::converter::registry::push_back(
            &map_dict_convertible,
            &map_convert_from_dict,
            boost::python::type_id<Map>()
        );
        
        boost::python::converter::registry::push_back(
            &fast_sequence_convertible<typename Map::value_type>,
            &map_convert_from_fast_sequence,
            boost::python::type_id<Map>()
        );
    }
};

#endif // guard

