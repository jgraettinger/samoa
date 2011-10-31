
#include <boost/python.hpp>
#include "samoa/persistence/persister.hpp"
#include "samoa/persistence/rolling_hash.hpp"
#include "samoa/persistence/record.hpp"
#include "samoa/datamodel/merge_func.hpp"
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
    const str_ptr_t & key,
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

future::ptr_t py_get(
    persister & p,
    const bpl::str & py_key)
{
    future::ptr_t f(boost::make_shared<future>());

    const char * buf = PyString_AS_STRING(py_key.ptr());
    str_ptr_t key = boost::make_shared<std::string>(
        buf, buf + PyString_GET_SIZE(py_key.ptr()));

    prec_ptr_t local_record = boost::make_shared<spb::PersistedRecord>();

    p.get(boost::bind(py_on_get, f, _1, key, local_record),
        *key, *local_record);
    return f; 
}

/////////// put support

void py_on_put(
    const future::ptr_t & future,
    const boost::system::error_code & ec,
    const datamodel::merge_result & result,
    const str_ptr_t & key,
    const prec_ptr_t & local_record)
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
    const bpl::object & merge_callable,
    spb::PersistedRecord & local_record,
    const spb::PersistedRecord & remote_record)
{
    pysamoa::python_scoped_lock block;

    bpl::reference_existing_object::apply<
        spb::PersistedRecord &>::type convert;

    bpl::reference_existing_object::apply<
        const spb::PersistedRecord &>::type const_convert;

    return bpl::extract<datamodel::merge_result>(
        merge_callable(
            bpl::handle<>(convert(local_record)),
            bpl::handle<>(const_convert(remote_record))));
}

future::ptr_t py_put(
    persister & p,
    const bpl::object & merge_callable,
    const bpl::str & py_key,
    const prec_ptr_t & remote_record)
{
    if(!PyCallable_Check(merge_callable.ptr()))
    {
        throw std::invalid_argument(
            "persister::put(merge_callback, key, remote_record): "\
            "argument 'merge_callback' isn't a callable");
    }

    future::ptr_t f(boost::make_shared<future>());
    f->set_reenter_via_post();

    const char * buf = PyString_AS_STRING(py_key.ptr());
    str_ptr_t key = boost::make_shared<std::string>(
        buf, buf + PyString_GET_SIZE(py_key.ptr()));

    prec_ptr_t local_record = boost::make_shared<spb::PersistedRecord>();

    p.put(
        boost::bind(&py_on_put, f, _1, _2, key, local_record),
        boost::bind(&py_on_merge, merge_callable, _1, _2),
        *key, *remote_record, *local_record);

    return f; 
}

/////////// drop support

bool py_on_drop(
    const future::ptr_t & future,
    const bpl::object & drop_callable,
    const str_ptr_t & key,
    const prec_ptr_t & record,
    bool found)
{
    pysamoa::python_scoped_lock block;

    if(found && bpl::extract<bool>(drop_callable(record))())
    {
        // the record is to be dropped
        // set the record instance as the yielded result,
        // and return true to the persister
        future->on_result(bpl::object(record));
        return true;
    }

    // yield None, and return false to the persister
    //   (record won't be dropped)
    future->on_result(bpl::object(prec_ptr_t()));
    return false;
}

future::ptr_t py_drop(
    persister & p,
    const bpl::object & drop_callable,
    const bpl::str & py_key)
{
    if(!PyCallable_Check(drop_callable.ptr()))
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
        boost::bind(&py_on_drop, f, drop_callable, key, local_record, _1),
        *key, *local_record);

    return f; 
}

/////////// iterate support

void py_on_iterate(const future::ptr_t & future, const record * record)
{
    pysamoa::python_scoped_lock block;

    bpl::reference_existing_object::apply<
        const samoa::persistence::record *>::type convert;
    bpl::object py_record(bpl::handle<>(convert(record)));

    SAMOA_ASSERT(future->is_yielded() && \
        "iteration future must be immediately callable");
    future->on_result(py_record);
}

future::ptr_t py_iterate(persister & p, size_t ticket)
{
    future::ptr_t f(boost::make_shared<future>());
    f->set_reenter_via_post();

    if(!p.iterate(boost::bind(&py_on_iterate, f, _1), ticket))
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

    boost::python::register_ptr_to_python<prec_ptr_t>();
}

}
}

