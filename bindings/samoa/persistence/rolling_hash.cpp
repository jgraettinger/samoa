
#include <boost/python.hpp>
#include "samoa/persistence/rolling_hash.hpp"
#include "samoa/persistence/record.hpp"
#include <memory>

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

void make_rolling_hash_bindings()
{
    bool (rolling_hash::*would_fit1)(size_t, size_t) = &rolling_hash::would_fit;
    bool (rolling_hash::*would_fit2)(size_t) = &rolling_hash::would_fit;

    // use of auto_ptr is non-optimal: unique_ptr is preferred
    bpl::class_<rolling_hash, std::auto_ptr<rolling_hash>, boost::noncopyable>(
            "RollingHash", bpl::no_init)

        .def("get", &py_get,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("prepare_record", &py_prepare_record,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("commit_record", &py_commit_record)
        .def("mark_for_deletion", &py_mark_for_deletion)
        .def("reclaim_head", &rolling_hash::reclaim_head)
        .def("rotate_head", &rolling_hash::rotate_head)
        .def("head", &rolling_hash::head,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("step", &rolling_hash::step,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("would_fit", would_fit1)
        .def("would_fit", would_fit2)
        .def("total_region_size", &rolling_hash::total_region_size)
        .def("used_region_size", &rolling_hash::used_region_size)
        .def("total_index_size", &rolling_hash::total_index_size)
        .def("used_index_size", &rolling_hash::used_index_size)
        .def("total_record_count", &rolling_hash::total_record_count)
        .def("live_record_count", &rolling_hash::live_record_count)
        .def("_dbg_begin", &rolling_hash::dbg_begin)
        .def("_dbg_end", &rolling_hash::dbg_end)
        .def("_dbg_wrap", &rolling_hash::dbg_wrap);
}

}
}

