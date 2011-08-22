
#include <boost/python.hpp>
#include "samoa/persistence/persister.hpp"
#include "samoa/persistence/rolling_hash.hpp"
#include "samoa/persistence/record.hpp"
#include "pysamoa/scoped_python.hpp"
#include "pysamoa/future.hpp"
#include <stdexcept>

namespace samoa {
namespace persistence {

namespace bpl = boost::python;
using namespace pysamoa;

/////////// get support

void py_on_get(
    const future::ptr_t & future,
    const boost::system::error_code & ec,
    const spb::PersistedRecord_ptr_t & record)
{
    python_scoped_lock block;

    if(ec)
    {
        future->on_error(ec);
        return;
    }

    future->on_result(bpl::object(record));
}

future::ptr_t py_get(
    persister & p,
    const std::string & key)
{
    future::ptr_t f(boost::make_shared<future>());
    f->set_reenter_via_post();

    p.get(boost::bind(py_on_get, f, _1, _2), key);
    return f; 
}

/////////// put support

void py_on_put(
    const future::ptr_t & future,
    const boost::system::error_code & ec,
    const spb::PersistedRecord_ptr_t & record)
{
    pysamoa::python_scoped_lock block;

    if(ec)
    {
        future->on_error(ec);
        return;
    }

    future->on_result(bpl::object(record));
    return;
}

spb::PersistedRecord_ptr_t py_on_merge(
    const bpl::object & merge_callable,
    const spb::PersistedRecord_ptr_t & cur_rec,
    const spb::PersistedRecord_ptr_t & new_rec)
{
    pysamoa::python_scoped_lock block;

    bpl::object result = merge_callable(cur_rec, new_rec);
    return bpl::extract<spb::PersistedRecord_ptr_t>(result);
}

future::ptr_t py_put(
    persister & p,
    const bpl::object & merge_callable,
    const std::string & key,
    const spb::PersistedRecord_ptr_t & new_record)
{
    if(!PyCallable_Check(merge_callable.ptr()))
    {
        throw std::invalid_argument(
            "persister::put(merge_callback, key, new_record): "\
            "argument 'merge_callback' isn't a callable");
    }

    future::ptr_t f(boost::make_shared<future>());
    f->set_reenter_via_post();

    p.put(
        boost::bind(&py_on_put, f, _1, _2),
        boost::bind(&py_on_merge, merge_callable, _1, _2),
        key, new_record);

    return f; 
}

/////////// drop support

void py_on_drop(
    const future::ptr_t & future,
    const boost::system::error_code & ec,
    const spb::PersistedRecord_ptr_t & record)
{
    pysamoa::python_scoped_lock block;

    if(ec)
    {
        future->on_error(ec);
        return;
    }

    future->on_result(bpl::object(record));
}

future::ptr_t py_drop(
    persister & p,
    const std::string & key)
{
    future::ptr_t f(boost::make_shared<future>());
    f->set_reenter_via_post();

    p.drop(boost::bind(&py_on_drop, f, _1, _2), key);
    return f; 
}

/////////// iterate support

void py_on_iterate(
    const future::ptr_t & future, 
    const boost::system::error_code & ec,
    const std::vector<const record *> & records)
{
    pysamoa::python_scoped_lock block;

    if(ec)
    {
        future->on_error(ec);
        return;
    }

    bpl::reference_existing_object::apply<
        const samoa::persistence::record *>::type convert;

    // allocate a tuple of containing python wrappers for each record
    bpl::tuple tuple(bpl::handle<>(PyTuple_New(records.size())));

    for(size_t i = 0; i != records.size(); ++i)
    {
        // PyTuple_SET_ITEM consumes reference created by convert
        PyTuple_SET_ITEM(tuple.ptr(), i, convert(records[i]));
    }

    future->on_result(tuple);
}

future::ptr_t py_iterate(
    persister & p, size_t ticket)
{
    future::ptr_t f(boost::make_shared<future>());
    f->set_reenter_via_post();

    if(!p.iterate(boost::bind(&py_on_iterate, f, _1, _2), ticket))
    {
        // iteration is complete; return an empty tuple

        f->on_result(bpl::tuple());
    }
    return f; 
}


void make_persister_bindings()
{
    bpl::class_<persister, persister::ptr_t, boost::noncopyable>(
        "Persister", bpl::init<>())
        .def("get", &py_get)
        .def("put", &py_put)
        .def("drop", &py_drop)
        .def("begin_iteration", &persister::begin_iteration)
        .def("iterate", &py_iterate)
        .def("add_heap_hash", &persister::add_heap_hash)
        .def("add_mapped_hash", &persister::add_mapped_hash)
        .def("get_layer_count", &persister::get_layer_count)
        .def("get_layer", &persister::get_layer,
            bpl::return_value_policy<bpl::reference_existing_object>());
}

}
}

