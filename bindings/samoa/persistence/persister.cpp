#include "pysamoa/boost_python.hpp"
#include "samoa/persistence/persister.hpp"
#include "samoa/persistence/rolling_hash/element.hpp"
#include "samoa/datamodel/merge_func.hpp"
#include "samoa/request/request_state.hpp"
#include "samoa/core/protobuf/fwd.hpp"
#include "pysamoa/scoped_python.hpp"
#include "pysamoa/future.hpp"
#include <memory>
#include <stdexcept>

namespace samoa {
namespace persistence {

namespace bpl = boost::python;
using namespace pysamoa;

typedef std::shared_ptr<std::string> str_ptr_t;
typedef std::shared_ptr<spb::PersistedRecord> prec_ptr_t;

/////////// get support

future::ptr_t py_get(persister & p, const bpl::str & py_key)
{
    future::ptr_t f(std::make_shared<future>());
    f->set_reenter_via_post();

    const char * buf = PyString_AS_STRING(py_key.ptr());
    str_ptr_t key = std::make_shared<std::string>(
        buf, buf + PyString_GET_SIZE(py_key.ptr()));

    prec_ptr_t local_record = std::make_shared<spb::PersistedRecord>();

    auto on_get = [f, key, local_record](bool found)
    {
        // key & local_record are captured to guard lifetime

        python_scoped_lock block;

        if(found)
        {
            f->on_result(bpl::object(local_record));
        }
        else
        {
            f->on_result(bpl::object(prec_ptr_t()));
        }
    };
    p.get(on_get, *key, *local_record);
    return f; 
}

/////////// drop support

future::ptr_t py_drop(persister & p,
    const bpl::object & drop_callback, const bpl::str & py_key)
{
    if(!PyCallable_Check(drop_callback.ptr()))
    {
        throw std::invalid_argument(
            "persister::drop(drop_callback, key): "\
            "argument 'drop_callback' isn't a callable");
    }

    future::ptr_t f(std::make_shared<future>());
    f->set_reenter_via_post();

    const char * buf = PyString_AS_STRING(py_key.ptr());
    str_ptr_t key = std::make_shared<std::string>(
        buf, buf + PyString_GET_SIZE(py_key.ptr()));

    prec_ptr_t local_record = std::make_shared<spb::PersistedRecord>();

    auto on_drop = [f, key, local_record, drop_callback](bool found)
    {
        pysamoa::python_scoped_lock block;

        if(found && bpl::extract<bool>(drop_callback(local_record))())
        {
            // the record is to be dropped; yield true, and
            //   return true to the persister
            f->on_result(bpl::object(true));
            return true;
        }

        // record isn't to be dropped; yield false, and
        //   return false to the persister
        f->on_result(bpl::object(false));
        return false;
    };
    p.drop(on_drop, *key, *local_record);
    return f; 
}

/////////// iteration support

future::ptr_t py_iteration_next(persister & p,
    const bpl::object & iteration_callback, unsigned ticket)
{
    if(!PyCallable_Check(iteration_callback.ptr()))
    {
        throw std::invalid_argument(
            "persister::iteration_next(iteration_callback, ticket): "\
            "argument 'iteration_callback' isn't a callable");
    }

    future::ptr_t f(std::make_shared<future>());
    f->set_reenter_via_post();

    auto on_next = [f, iteration_callback](rolling_hash::element element)
    {
        pysamoa::python_scoped_lock block;

        if(element.is_null())
        {
            // no more elements; yield false and don't callback
            f->on_result(bpl::object(false));
        }
        else
        {
            // have another element; callback & yield true
            iteration_callback(element);
            f->on_result(bpl::object(true));
        }
    };
    p.iteration_next(on_next, ticket);
    return f; 
}

/////////// put support

future::ptr_t py_put(
    persister & p,
    const bpl::object & merge_callback,
    const bpl::str & py_key,
    const prec_ptr_t & remote_record)
{
    if(merge_callback && !PyCallable_Check(merge_callback.ptr()))
    {
        throw std::invalid_argument(
            "persister::put(merge_callback, key, remote_record): "\
            "argument 'merge_callback' must be None or a callable");
    }

    future::ptr_t f(std::make_shared<future>());
    f->set_reenter_via_post();

    const char * buf = PyString_AS_STRING(py_key.ptr());
    str_ptr_t key = std::make_shared<std::string>(
        buf, buf + PyString_GET_SIZE(py_key.ptr()));

    prec_ptr_t local_record = std::make_shared<spb::PersistedRecord>();

    auto on_put = [f, key, local_record, remote_record](
        const boost::system::error_code & ec,
        const datamodel::merge_result & merge_result,
        const core::murmur_checksum_t & checksum)
    {
        // guard key, local_record, remote_record lifetime

        pysamoa::python_scoped_lock block;

        if(ec)
        {
            f->on_error(ec);
            return;
        }
        f->on_result(bpl::make_tuple(merge_result,
            bpl::make_tuple(checksum[0], checksum[1])));
        return;
    };
        
    auto on_merge = [merge_callback](
        spb::PersistedRecord & local_record,
        const spb::PersistedRecord & remote_record) -> datamodel::merge_result
    {
        pysamoa::python_scoped_lock block;
    
        datamodel::merge_result result(true, false);
    
        if(merge_callback)
        {
            bpl::reference_existing_object::apply<
                spb::PersistedRecord &>::type convert;
    
            bpl::reference_existing_object::apply<
                const spb::PersistedRecord &>::type const_convert;
    
            result = bpl::extract<datamodel::merge_result>(
                merge_callback(
                    bpl::handle<>(convert(local_record)),
                    bpl::handle<>(const_convert(remote_record))));
        }
        return result;
    };
    p.put(on_put, on_merge, *key, *remote_record, *local_record);
    return f; 
}

/////////// bottom_up_compaction support

future::ptr_t py_bottom_up_compaction(persister & p)
{
    future::ptr_t f(std::make_shared<future>());
    f->set_reenter_via_post();

    auto on_compaction = [f]()
    {
        pysamoa::python_scoped_lock block;
        f->on_result(bpl::object());
    };
    p.bottom_up_compaction(on_compaction);
    return f; 
}

/////////// set_prune_callback support

void py_set_prune_callback(persister & p,
    const bpl::object & prune_callback)
{
    auto callback = [prune_callback](
        spb::PersistedRecord & record) -> bool
    {
        pysamoa::python_scoped_lock block;
        return bpl::extract<bool>(prune_callback(record));
    };
    p.set_prune_callback(callback);
}

/////////// set_upkeep_callback support

void py_set_upkeep_callback(persister & p,
    const bpl::object & upkeep_callback)
{
    auto callback = [upkeep_callback](
        const request::state::ptr_t & rstate,
        const core::murmur_checksum_t &,
        const core::murmur_checksum_t &)
    {
        pysamoa::python_scoped_lock block;
        upkeep_callback(rstate);
    };
    p.set_upkeep_callback(callback);
}

///////////

void make_persister_bindings()
{
    bpl::class_<persister, persister::ptr_t, boost::noncopyable>(
        "Persister", bpl::init<>())
        .def("layer_count", &persister::layer_count)
        .def("layer", &persister::layer,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("leaf_layer", &persister::leaf_layer,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("add_heap_hash_ring", &persister::add_heap_hash_ring)
        .def("add_mapped_hash_ring", &persister::add_mapped_hash_ring)
        .def("get", &py_get)
        .def("drop", &py_drop)
        .def("iteration_begin", &persister::iteration_begin)
        .def("iteration_next", &py_iteration_next)
        .def("put", &py_put)
        .def("set_prune_callback", &py_set_prune_callback)
        .def("set_upkeep_callback", &py_set_upkeep_callback)
        .def("max_compaction_factor",
            &persister::max_compaction_factor)
        .def("bottom_up_compaction", &py_bottom_up_compaction)
        .def("total_storage", &persister::total_storage)
        .def("used_storage", &persister::used_storage)
        ;

    boost::python::register_ptr_to_python<prec_ptr_t>();
}

}
}

