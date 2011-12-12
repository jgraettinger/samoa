
#include <boost/python.hpp>
#include "samoa/persistence/rolling_hash/hash_ring.hpp"
#include "samoa/persistence/rolling_hash/packet.hpp"
#include <memory>

namespace samoa {
namespace persistence {
namespace rolling_hash {

namespace bpl = boost::python;

/*
const record * py_get(rolling_hash * hash, const bpl::str & key)
{
    const char * key_begin = PyString_AsString(key.ptr());
    const char * key_end = key_begin + PyString_GET_SIZE(key.ptr());

    return hash->get(key_begin, key_end);
}

record * py_prepare_record(rolling_hash * hash, const bpl::str & key,
    unsigned value_length)
{
    const char * key_begin = PyString_AsString(key.ptr());
    const char * key_end = key_begin + PyString_GET_SIZE(key.ptr());

    return hash->prepare_record(key_begin, key_end, value_length);
}

void py_commit_record(rolling_hash * hash)
{ hash->commit_record(); }

bool py_mark_for_deletion(rolling_hash * hash, const bpl::str & key)
{
    const char * key_begin = PyString_AsString(key.ptr());
    const char * key_end = key_begin + PyString_GET_SIZE(key.ptr());

    return hash->mark_for_deletion(key_begin, key_end);
}
*/

std::string py_repr_packet(packet & p);

std::string py_repr_hash_ring(hash_ring & r)
{
    std::stringstream out;

    out << "hash_ring@" << &r << "<";

    out << "region " << r.region_size() << ", ";
    out << "index " << r.index_size() << ", ";
    out << "ring offset " << r.ring_region_offset() << ", ";
    out << "begin " << r.begin_offset() << ", ";
    out << "end " << r.end_offset() << ", ";

    if(r.is_wrapped())
        out << "wrapped,\n";
    else
        out << "not wrapped,\n";

    packet * pkt = r.head();
    for(unsigned i = 0; pkt; ++i)
    {
        uint32_t offset = r.packet_offset(pkt);
        out << "\t" << offset << ":" << (offset + pkt->packet_length());
        out << " " << py_repr_packet(*pkt) << ",\n";

        pkt = r.next_packet(pkt);
    }
    out << ">";

    return out.str();
}

std::string py_repr_locator(hash_ring::locator & l)
{
    std::stringstream out;

    out << "locator@" << &l << "<";

    out << "index_location " << l.index_location << ", ";

    out << "previous_chained_head ";
    if(l.previous_chained_head)
    {
        out << py_repr_packet(*l.previous_chained_head) << ", ";
    }
    else
    {
        out << "None, ";
    }

    out << "element_head ";
    if(l.element_head)
    {
        out << py_repr_packet(*l.element_head);
    }
    else
    {
        out << "None";
    }
    out << ">";

    return out.str();
}

void make_hash_ring_bindings()
{
    bpl::class_<hash_ring::locator>("HashRing_Locator", bpl::no_init)
        .def_readonly("index_location", &hash_ring::locator::index_location)
        .add_property("previous_chained_head",
            bpl::make_getter(&hash_ring::locator::previous_chained_head,
                bpl::return_value_policy<bpl::reference_existing_object>()))
        .add_property("element_head",
            bpl::make_getter(&hash_ring::locator::element_head,
                bpl::return_value_policy<bpl::reference_existing_object>()))
        .def("__repr__", &py_repr_locator)
        ;

    hash_ring::locator (hash_ring::*locate_key_ptr)(
        const std::string &) const = &hash_ring::locate_key;

    // use of auto_ptr is non-optimal: unique_ptr is preferred
    bpl::class_<hash_ring, boost::noncopyable>(
            "HashRing", bpl::no_init)

        .def("__repr__", &py_repr_hash_ring)
        .def("locate_key", locate_key_ptr)
        .def("allocate_packets", &hash_ring::allocate_packets,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("reclaim_head", &hash_ring::reclaim_head)
        .def("update_hash_chain", &hash_ring::update_hash_chain)
        .def("drop_from_hash_chain", &hash_ring::drop_from_hash_chain)
        .def("head", &hash_ring::head,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("next_packet", &hash_ring::next_packet,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("packet_offset", &hash_ring::packet_offset)
        .def("region_size", &hash_ring::region_size)
        .def("index_size", &hash_ring::index_size)
        .def("index_region_offset", &hash_ring::index_region_offset)
        .def("ring_region_offset", &hash_ring::ring_region_offset)
        .def("begin_offset", &hash_ring::begin_offset)
        .def("end_offset", &hash_ring::end_offset)
        .def("is_wrapped", &hash_ring::is_wrapped)
        ;

    /*
        .def("total_region_size", &rolling_hash::total_region_size)
        .def("used_region_size", &rolling_hash::used_region_size)
        .def("total_index_size", &rolling_hash::total_index_size)
        .def("used_index_size", &rolling_hash::used_index_size)
        .def("total_record_count", &rolling_hash::total_record_count)
        .def("live_record_count", &rolling_hash::live_record_count)
        .def("_dbg_begin", &rolling_hash::dbg_begin)
        .def("_dbg_end", &rolling_hash::dbg_end)
        .def("_dbg_wrap", &rolling_hash::dbg_wrap);
    */
}

}
}
}

