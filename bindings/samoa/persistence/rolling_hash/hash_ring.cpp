
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

void make_hash_ring_bindings()
{
    hash_ring::locator (hash_ring::*locate_key_ptr)(
        const std::string &) const = &hash_ring::locate_key;

    // use of auto_ptr is non-optimal: unique_ptr is preferred
    bpl::class_<hash_ring, boost::noncopyable>(
            "HashRing", bpl::no_init)

        .def("locate_key", locate_key_ptr)
        .def("allocate_packets", &hash_ring::allocate_packets,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("reclaim_head", &hash_ring::reclaim_head)
        .def("rotate_head", &hash_ring::rotate_head)
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

