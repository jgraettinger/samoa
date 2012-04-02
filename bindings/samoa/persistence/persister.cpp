
#include <boost/python.hpp>
#include "samoa/persistence/persister.hpp"
#include "samoa/persistence/rolling_hash/element.hpp"
#include "samoa/datamodel/merge_func.hpp"
#include "samoa/request/request_state.hpp"
#include "samoa/core/protobuf/fwd.hpp"
#include "pysamoa/scoped_python.hpp"
#include "pysamoa/future.hpp"
#include <stdexcept>

namespace samoa {
namespace persistence {

namespace bpl = boost::python;
using namespace pysamoa;

typedef boost::shared_ptr<std::string> str_ptr_t;
typedef boost::shared_ptr<spb::PersistedRecord> prec_ptr_t;

/////////// get support

void py_on_get(
    const future::ptr_t & future,
    bool found,
    const str_ptr_t & /*key_lifetime_guard*/,
    const prec_ptr_t & record)
{
    python_scoped_lock block;

    if(found)
    {
        future->on_result(bpl::object(record));
    }
    else
    {
        future->on_result(bpl::object(prec_ptr_t()));
    }
}

future::ptr_t py_get(persister & p, const bpl::str & py_key)
{
    future::ptr_t f(boost::make_shared<future>());
    f->set_reenter_via_post();

    const char * buf = PyString_AS_STRING(py_key.ptr());
    str_ptr_t key = boost::make_shared<std::string>(
        buf, buf + PyString_GET_SIZE(py_key.ptr()));

    prec_ptr_t local_record = boost::make_shared<spb::PersistedRecord>();

    p.get(boost::bind(py_on_get, f, _1, key, local_record),
        *key, *local_record);
    return f; 
}

/////////// drop support

bool py_on_drop(
    const future::ptr_t & future,
    const bpl::object & drop_callback,
    const str_ptr_t & /*key_lifetime_guard*/,
    const prec_ptr_t & record,
    bool found)
{
    pysamoa::python_scoped_lock block;

    if(found && bpl::extract<bool>(drop_callback(record))())
    {
        // the record is to be dropped; yield true, and
        //   return true to the persister
        future->on_result(bpl::object(true));
        return true;
    }

    // record isn't to be dropped; yield false, and
    //   return false to the persister
    future->on_result(bpl::object(false));
    return false;
}

future::ptr_t py_drop(persister & p,
    const bpl::object & drop_callback, const bpl::str & py_key)
{
    if(!PyCallable_Check(drop_callback.ptr()))
    {
        throw std::invalid_argument(
            "persister::drop(drop_callback, key): "\
            "argument 'drop_callback' isn't a callable");
    }

    future::ptr_t f(boost::make_shared<future>());
    f->set_reenter_via_post();

    const char * buf = PyString_AS_STRING(py_key.ptr());
    str_ptr_t key = boost::make_shared<std::string>(
        buf, buf + PyString_GET_SIZE(py_key.ptr()));

    prec_ptr_t local_record = boost::make_shared<spb::PersistedRecord>();

    p.drop(
        boost::bind(&py_on_drop, f, drop_callback, key, local_record, _1),
        *key, *local_record);

    return f; 
}

/////////// iteration support

void py_on_iteration_next(
    const future::ptr_t & future,
    const bpl::object & iteration_callback,
    rolling_hash::element element)
{
    pysamoa::python_scoped_lock block;

    if(element.is_null())
    {
        // no more elements; yield false and don't callback
        future->on_result(bpl::object(false));
    }
    else
    {
        // have another element; callback & yield true
        iteration_callback(element);
        future->on_result(bpl::object(true));
    }
}

future::ptr_t py_iteration_next(persister & p,
    const bpl::object & iteration_callback, unsigned ticket)
{
    if(!PyCallable_Check(iteration_callback.ptr()))
    {
        throw std::invalid_argument(
            "persister::iteration_next(iteration_callback, ticket): "\
            "argument 'iteration_callback' isn't a callable");
    }

    future::ptr_t f(boost::make_shared<future>());
    f->set_reenter_via_post();

    p.iteration_next(
        boost::bind(&py_on_iteration_next, f, iteration_callback, _1),
        ticket);

    return f; 
}

/////////// put support

void py_on_put(
    const future::ptr_t & future,
    const boost::system::error_code & ec,
    const datamodel::merge_result & result,
    const str_ptr_t & /*key_lifetime_guard*/,
    const prec_ptr_t & /*local_record_lifetime_guard*/)
{
    pysamoa::python_scoped_lock block;

    if(ec)
    {
        future->on_error(ec);
        return;
    }

    future->on_result(bpl::object(result));
    return;
}

datamodel::merge_result py_on_merge(
    const bpl::object & merge_callback,
    spb::PersistedRecord & local_record,
    const spb::PersistedRecord & remote_record)
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
}

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

    future::ptr_t f(boost::make_shared<future>());
    f->set_reenter_via_post();

    const char * buf = PyString_AS_STRING(py_key.ptr());
    str_ptr_t key = boost::make_shared<std::string>(
        buf, buf + PyString_GET_SIZE(py_key.ptr()));

    prec_ptr_t local_record = boost::make_shared<spb::PersistedRecord>();

    p.put(
        boost::bind(&py_on_put, f, _1, _2, key, local_record),
        boost::bind(&py_on_merge, merge_callback, _1, _2),
        *key, *remote_record, *local_record);

    return f; 
}

/////////// bottom_up_compaction support

void py_on_bottom_up_compaction(const future::ptr_t & future)
{
    pysamoa::python_scoped_lock block;
    future->on_result(bpl::object());
}

future::ptr_t py_bottom_up_compaction(persister & p)
{
    future::ptr_t f(boost::make_shared<future>());
    f->set_reenter_via_post();

    p.bottom_up_compaction(boost::bind(&py_on_bottom_up_compaction, f));
    return f; 
}

/////////// set_prune_callback support

bool py_on_prune_callback(
    const bpl::object & prune_callback,
    spb::PersistedRecord & record)
{
    pysamoa::python_scoped_lock block;

    return bpl::extract<bool>(prune_callback(record));
}

void py_set_prune_callback(persister & p,
    const bpl::object & prune_callback)
{
    datamodel::prune_func_t callback = \
        boost::bind(&py_on_prune_callback,
            prune_callback, _1);

    p.set_prune_callback(callback);
}

/////////// set_upkeep_callback support

void py_on_upkeep_callback(
    const bpl::object & upkeep_callback,
    const request::state::ptr_t & rstate)
{
    pysamoa::python_scoped_lock block;
    upkeep_callback(rstate);
}

void py_set_upkeep_callback(persister & p,
    const bpl::object & upkeep_callback)
{
    persister::upkeep_callback_t callback = \
        boost::bind(&py_on_upkeep_callback,
            upkeep_callback, _1);

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

