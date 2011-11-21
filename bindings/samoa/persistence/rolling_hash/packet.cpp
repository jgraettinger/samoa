
#include <boost/python.hpp>
#include "samoa/persistence/rolling_hash/packet.hpp"

namespace samoa {
namespace persistence {
namespace rolling_hash {

namespace bpl = boost::python;

bpl::str py_key(packet * p)
{
    return bpl::str(p->key_begin(), p->key_end());
}

void py_set_key(packet * p, const bpl::str & key)
{
    const char * begin = PyString_AsString(key.ptr());
    const char * end = begin + PyString_GET_SIZE(key.ptr());

    std::copy(begin, end, p->set_key(std::distance(begin, end)));
}

bpl::str py_value(packet * p)
{
    return bpl::str(p->value_begin(), p->value_end());
}

void py_set_value(packet * p, const bpl::str & value)
{
    const char * begin = PyString_AsString(value.ptr());
    const char * end = begin + PyString_GET_SIZE(value.ptr());

    std::copy(begin, end, p->set_value(std::distance(begin, end)));
}


void make_packet_bindings()
{
    bpl::class_<packet, boost::noncopyable>("Packet", bpl::no_init)
        .def("check_integrity", &packet::check_integrity)
        .def("compute_crc_32", &packet::compute_crc_32)
        .def("set_crc_32", &packet::set_crc_32)
        .def("hash_chain_next", &packet::hash_chain_next)
        .def("set_hash_chain_next", &packet::set_hash_chain_next)
        .def("is_dead", &packet::is_dead)
        .def("set_dead", &packet::set_dead)
        .def("continues_sequence", &packet::continues_sequence)
        .def("set_continues_sequence", &packet::set_continues_sequence)
        .def("completes_sequence", &packet::completes_sequence)
        .def("set_completes_sequence", &packet::set_completes_sequence)
        .def("capacity", &packet::capacity)
        .def("available_capacity", &packet::available_capacity)
        .def("key_length", &packet::key_length)
        .def("key", &py_key)
        .def("set_key", &py_set_key)
        .def("value_length", &packet::value_length)
        .def("value", &py_value)
        .def("set_value", &py_set_value)
        .def("packet_length", &packet::packet_length)
        .def("header_length", &packet::header_length)
        .staticmethod("header_length")
        ;
}

}
}
}

