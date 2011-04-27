
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
    const bpl::object & callable,
    const boost::system::error_code & ec,
    const record * record)
{
    python_scoped_lock block;

    if(ec)
    {
        future->on_error(ec);
        return;
    }

    bpl::reference_existing_object::apply<
        const samoa::persistence::record *>::type convert;

    bpl::object arg = (record ? \
        bpl::object(bpl::handle<>(convert(record))) : bpl::object());

    bpl::object result = callable(arg);

    // set the future to re-enter coroutine via post, so that
    //  on_result is guarentted to return immediately
    future->set_reenter_via_post();
    future->on_result(result);
}

future::ptr_t py_get(
    persister & p,
    const bpl::object & callable,
    const std::string & key)
{
    if(!PyCallable_Check(callable.ptr()))
    {
        throw std::invalid_argument("persister::get(key, callable): "\
            "argument 'callable' isn't a callable");
    }

    future::ptr_t f(boost::make_shared<future>());

    p.get(boost::bind(py_on_get, f, callable, _1, _2), std::string(key));
    return f; 
}

/////////// put support

bool py_on_put(
    const future::ptr_t & future,
    const bpl::object & callable,
    const boost::system::error_code & ec,
    const record * current_record,
    record * new_record)
{
    pysamoa::python_scoped_lock block;

    if(ec)
    {
        future->on_error(ec);
        return false;
    }

    bpl::reference_existing_object::apply<
        const samoa::persistence::record *>::type cconvert;
    bpl::reference_existing_object::apply<
        samoa::persistence::record *>::type convert;

    bpl::object arg = bpl::make_tuple(
        (current_record ? \
            bpl::object(bpl::handle<>(cconvert(current_record))) : \
            bpl::object()),
        bpl::handle<>(convert(new_record)));

    bpl::object result = callable(*arg);

    bool committed = bpl::extract<bool>(result)();

    // set the future to re-enter coroutine via post, so that
    //  on_result is guarentted to return immediately
    future->set_reenter_via_post();
    future->on_result(result);

    return committed;
}

future::ptr_t py_put(
    persister & p,
    const bpl::object & callable,
    const std::string & key,
    unsigned value_length)
{
    if(!PyCallable_Check(callable.ptr()))
    {
        throw std::invalid_argument("persister::put(key, val_len, callable): "\
            "argument 'callable' isn't a callable");
    }

    future::ptr_t f(boost::make_shared<future>());

    p.put(boost::bind(&py_on_put, f, callable, _1, _2, _3),
        std::string(key), value_length );
    return f; 
}

/////////// drop support

bool py_on_drop(
    const future::ptr_t & future,
    const bpl::object & callable,
    const boost::system::error_code & ec,
    const record * record)
{
    pysamoa::python_scoped_lock block;

    if(ec)
    {
        future->on_error(ec);
        return false;
    }

    bpl::reference_existing_object::apply<
        const samoa::persistence::record *>::type convert;

    bpl::object arg = (record ? \
        bpl::object(bpl::handle<>(convert(record))) : bpl::object());

    bpl::object result = callable(arg);

    bool committed = bpl::extract<bool>(result)();

    // set the future to re-enter coroutine via post, so that
    //  on_result is guarentted to return immediately
    future->set_reenter_via_post();
    future->on_result(result);

    return committed;
}

future::ptr_t py_drop(
    persister & p,
    const bpl::object & callable,
    const std::string & key)
{
    if(!PyCallable_Check(callable.ptr()))
    {
        throw std::invalid_argument("persister::drop(key, callable): "\
            "argument 'callable' isn't a callable");
    }

    future::ptr_t f(boost::make_shared<future>());

    p.drop(boost::bind(&py_on_drop, f, callable, _1, _2),
        std::string(key));
    return f; 
}



void make_persister_bindings()
{
    bpl::class_<persister, persister::ptr_t, boost::noncopyable>(
        "Persister", bpl::init<core::proactor &>())
        .def("get", &py_get)
        .def("put", &py_put)
        .def("drop", &py_drop)
        .def("add_heap_hash", &persister::add_heap_hash)
        .def("add_mapped_hash", &persister::add_mapped_hash)
        .def("get_layer_count", &persister::get_layer_count)
        .def("get_layer", &persister::get_layer,
            bpl::return_value_policy<bpl::reference_existing_object>());
}

}
}

