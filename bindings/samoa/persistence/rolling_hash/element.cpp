
#include <boost/python.hpp>
#include "samoa/persistence/rolling_hash/element.hpp"

#include "samoa/persistence/rolling_hash/key_gather_iterator.hpp"
#include "samoa/persistence/rolling_hash/value_gather_iterator.hpp"

namespace samoa {
namespace persistence {
namespace rolling_hash {

namespace bpl = boost::python;


element * py_element_with_key(const hash_ring * ring, packet * pkt,
    const bpl::str & key, uint32_t hash_chain_next)
{
    return new element(ring, pkt,
        PyString_GET_SIZE(key.ptr()), PyString_AS_STRING(key.ptr()),
        hash_chain_next);
}

element * py_element_with_key_value(const hash_ring * ring, packet * pkt,
    const bpl::str & key, const bpl::str & value, uint32_t hash_chain_next)
{
    return new element(ring, pkt,
        PyString_GET_SIZE(key.ptr()), PyString_AS_STRING(key.ptr()),
        PyString_GET_SIZE(value.ptr()), PyString_AS_STRING(value.ptr()),
        hash_chain_next);
}

void py_set_value(element & elem, const bpl::str & value)
{
    elem.set_value(
        PyString_GET_SIZE(value.ptr()), PyString_AS_STRING(value.ptr()));
}

std::string py_key(element & elem)
{
    return std::string(key_gather_iterator(elem), key_gather_iterator());
}

std::string py_value(element & elem)
{
    return std::string(value_gather_iterator(elem), value_gather_iterator());
}

void make_element_bindings()
{
    bpl::class_<element>("Element",
            bpl::init<const hash_ring *, packet *>())
        .def("__init__", bpl::make_constructor(&py_element_with_key,
            bpl::default_call_policies(),
            (bpl::arg("hash_ring"), bpl::arg("head"), bpl::arg("key"),
                bpl::arg("hash_chain_next") = 0)))
        .def("__init__", bpl::make_constructor(&py_element_with_key_value,
            bpl::default_call_policies(),
            (bpl::arg("hash_ring"), bpl::arg("head"), bpl::arg("key"),
                bpl::arg("value"), bpl::arg("hash_chain_next") = 0)))
        .def("key_length", &element::key_length)
        .def("value_length", &element::value_length)
        .def("capacity", &element::capacity)
        .def("set_value", &py_set_value)
        .def("head", &element::head,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("step", &element::step,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("key", &py_key)
        .def("value", &py_value)
        ;
}

}
}
}

