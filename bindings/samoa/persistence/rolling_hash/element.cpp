
#include <boost/python.hpp>
#include "samoa/persistence/rolling_hash/element.hpp"
#include "samoa/persistence/rolling_hash/key_gather_iterator.hpp"
#include "samoa/persistence/rolling_hash/value_gather_iterator.hpp"
#include "samoa/persistence/rolling_hash/value_zco_adapter.hpp"
#include "samoa/persistence/rolling_hash/value_zci_adapter.hpp"
#include "samoa/core/protobuf/fwd.hpp"
#include "samoa/core/protobuf/samoa.pb.h"

namespace samoa {
namespace persistence {
namespace rolling_hash {

namespace bpl = boost::python;
namespace spb = samoa::core::protobuf;

element * py_element_with_key_value(const hash_ring * ring,
    packet * pkt, const bpl::str & key, const bpl::str & value)
{
    return new element(ring, pkt,
        PyString_GET_SIZE(key.ptr()), PyString_AS_STRING(key.ptr()),
        PyString_GET_SIZE(value.ptr()), PyString_AS_STRING(value.ptr()));
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

bool py_parse_persisted_record(
    element & elem,
    const boost::shared_ptr<spb::PersistedRecord> & record)
{
    //LOG_INFO("parsing element " << elem.head());

    rolling_hash::value_zci_adapter zci_adapter(elem);
    return record->ParseFromZeroCopyStream(&zci_adapter);
}

bool py_write_persisted_record(
    element & elem,
    const boost::shared_ptr<spb::PersistedRecord> & record)
{
    rolling_hash::value_zco_adapter zco_adapter(elem);
    bool result = record->SerializeToZeroCopyStream(&zco_adapter);
    zco_adapter.finish();

    return result;
}

void make_element_bindings()
{
    bpl::class_<element>("Element",
            bpl::init<const hash_ring *, packet *>())
        .def(bpl::init<const hash_ring *, packet *, const std::string &>())
        .def("__init__", bpl::make_constructor(&py_element_with_key_value,
            bpl::default_call_policies(),
            (bpl::arg("hash_ring"), bpl::arg("head"),
                bpl::arg("key"), bpl::arg("value"))))
        .def("key_length", &element::key_length)
        .def("value_length", &element::value_length)
        .def("capacity", &element::capacity)
        .def("set_value", &py_set_value)
        .def("set_dead", &element::set_dead)
        .def("parse_persisted_record", &py_parse_persisted_record)
        .def("write_persisted_record", &py_write_persisted_record)
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

