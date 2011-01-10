
#include "samoa/rolling_hash.hpp"
#include <boost/python.hpp>

namespace samoa {

namespace bpl = boost::python;

struct py_rolling_hash_iter
{
    rolling_hash * hash;
    const rolling_hash::record * cur;

    bpl::tuple next()
    {
        while(cur && cur->is_dead())
            cur = hash->step(cur);

        if(!cur)
        {
            PyErr_SetObject(PyExc_StopIteration, Py_None);
            boost::python::throw_error_already_set();
        }

        bpl::tuple ret = bpl::make_tuple(
            bpl::str(cur->key(), cur->key() + cur->key_length()),
            bpl::str(cur->value(), cur->value() + cur->value_length()));

        cur = hash->step(cur);

        return ret;
    }
};

py_rolling_hash_iter py_iter(rolling_hash * hash)
{
    py_rolling_hash_iter iter;
    iter.hash = hash;
    iter.cur = hash->head();
    return iter;
}

bpl::object py_get(rolling_hash * hash, const bpl::str & key)
{
    const char * key_begin = PyString_AsString(key.ptr());
    const char * key_end = key_begin + PyString_GET_SIZE(key.ptr());

    const rolling_hash::record * rec = hash->get(key_begin, key_end);
    if(rec)
        return bpl::str(rec->value(), rec->value() + rec->value_length());
    else
        return bpl::object();
}

void py_set(rolling_hash * hash, const bpl::str & key, const bpl::str & val)
{
    const char * key_begin = PyString_AsString(key.ptr());
    const char * key_end = key_begin + PyString_GET_SIZE(key.ptr());

    const char * val_begin = PyString_AsString(val.ptr());
    const char * val_end = val_begin + PyString_GET_SIZE(val.ptr());

    hash->set(key_begin, key_end, val_begin, val_end);
}

bool py_drop(rolling_hash * hash, const bpl::str & key)
{
    const char * key_begin = PyString_AsString(key.ptr());
    const char * key_end = key_begin + PyString_GET_SIZE(key.ptr());

    return hash->drop(key_begin, key_end);
}

bpl::object py_head(rolling_hash * hash)
{
    const rolling_hash::record * head = hash->head();

    if(head && !head->is_dead())
        return bpl::make_tuple(
            bpl::str(head->key(), head->key() + head->key_length()),
            bpl::str(head->value(), head->value() + head->value_length()));

    return bpl::object();
}

void make_rolling_hash_bindings()
{
    bpl::class_<py_rolling_hash_iter>("__RollingHash_iterator", bpl::no_init)
        .def("next", &py_rolling_hash_iter::next);

    bpl::class_<rolling_hash, boost::noncopyable>("RollingHash", bpl::no_init)
        .def("__iter__", &py_iter)
        .def("get", &py_get)
        .def("would_fit", &rolling_hash::would_fit)
        .def("set", &py_set)
        .def("drop", &py_drop)
        .def("head", &py_head)
        .def("migrate_head", &rolling_hash::migrate_head)
        .def("drop_head", &rolling_hash::drop_head)
        .add_property("total_region_size", &rolling_hash::total_region_size)
        .add_property("used_region_size", &rolling_hash::used_region_size)
        .add_property("total_index_size", &rolling_hash::total_index_size)
        .add_property("used_index_size", &rolling_hash::used_index_size);
}

}

