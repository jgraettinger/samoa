
#include "samoa/persistence/rolling_hash.hpp"
#include "samoa/persistence/record.hpp"
#include <boost/python.hpp>

namespace samoa {
namespace persistence {

namespace bpl = boost::python;

const record * py_get(rolling_hash * hash, const bpl::str & key)
{
    const char * key_begin = PyString_AsString(key.ptr());
    const char * key_end = key_begin + PyString_GET_SIZE(key.ptr());

    return hash->get(key_begin, key_end);
}

record * py_prepare_record(rolling_hash * hash, const bpl::str & key,
    unsigned value_length_upper_bound)
{
    const char * key_begin = PyString_AsString(key.ptr());
    const char * key_end = key_begin + PyString_GET_SIZE(key.ptr());

    return hash->prepare_record(key_begin, key_end, value_length_upper_bound);
}

void py_commit_record(rolling_hash * hash, unsigned actual_value_length)
{ hash->commit_record(actual_value_length); }

bool py_mark_for_deletion(rolling_hash * hash, const bpl::str & key)
{
    const char * key_begin = PyString_AsString(key.ptr());
    const char * key_end = key_begin + PyString_GET_SIZE(key.ptr());

    return hash->mark_for_deletion(key_begin, key_end);
}

void make_rolling_hash_bindings()
{
    bpl::class_<rolling_hash, boost::noncopyable>("RollingHash", bpl::no_init)
        .def("get", &py_get,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("prepare_record", &py_prepare_record,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("commit_record", &py_commit_record)
        .def("mark_for_deletion", &py_mark_for_deletion)
        .def("reclaim_head", &rolling_hash::reclaim_head)
        .def("head", &rolling_hash::head,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("step", &rolling_hash::step,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("would_fit", &rolling_hash::would_fit)
        .def("total_region_size", &rolling_hash::total_region_size)
        .def("used_region_size", &rolling_hash::used_region_size)
        .def("total_index_size", &rolling_hash::total_index_size)
        .def("used_index_size", &rolling_hash::used_index_size)
        .def("total_record_count", &rolling_hash::total_record_count)
        .def("live_record_count", &rolling_hash::live_record_count);
}

}
}

