
#include <boost/python.hpp>
#include "samoa/persistence/record.hpp"

namespace samoa {
namespace persistence {

namespace bpl = boost::python;

bpl::str py_get_key(record * r)
{ return bpl::str(r->key_begin(), r->key_end()); }

bpl::str py_get_value(record * r)
{ return bpl::str(r->value_begin(), r->value_end()); }

void py_set_value(record * r, const bpl::str & val)
{
    record::offset_t val_len = (record::offset_t) PyString_GET_SIZE(val.ptr());

    if(val_len > r->value_length())
        throw std::overflow_error("value overflow");

    const char * val_begin = PyString_AsString(val.ptr());
    const char * val_end = val_begin + val_len;

    std::copy(val_begin, val_end, r->value_begin());
    r->trim_value_length(val_len);
}

void make_record_bindings()
{
    bpl::class_<record, boost::noncopyable>("Record", bpl::no_init)
        .def("key_length", &record::key_length)
        .def("value_length", &record::value_length)
        .def("is_dead", &record::is_dead)
        .def("is_copy", &record::is_copy)
        .add_property("key", &py_get_key)
        .add_property("value", &py_get_value)
        .def("set_value", &py_set_value);
}

}
}

