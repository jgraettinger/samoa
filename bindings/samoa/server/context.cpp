
#include <boost/python.hpp>
#include "samoa/server/context.hpp"
#include "samoa/server/cluster_state.hpp"
#include "samoa/client/server_pool.hpp"
#include "pysamoa/scoped_python.hpp"
#include "pysamoa/future.hpp"
#include <stdexcept>

namespace samoa {
namespace server {

namespace bpl = boost::python;
using namespace pysamoa;

///////////// cluster_state_transaction support

bool py_on_cluster_state_transaction(
    const future::ptr_t & future,
    const bpl::object & callable,
    spb::ClusterState & state)
{
    python_scoped_lock block;

    // wrap python reference to state
    bpl::reference_existing_object::apply<
        spb::ClusterState &>::type convert;
    bpl::object arg(bpl::handle<>(convert(state)));

    bpl::object result = callable(arg);
    bool committed = bpl::extract<bool>(result);

    future->set_reenter_via_post();
    future->on_result(result);

    return committed;
}

future::ptr_t py_cluster_state_transaction(
    context & c,
    const bpl::object & callable)
{
    if(!PyCallable_Check(callable.ptr()))
    {
        throw std::invalid_argument(
            "context::cluster_state_transaction(callable): "\
            "argument must be a callable");
    }

    future::ptr_t f(boost::make_shared<future>());

    c.cluster_state_transaction(boost::bind(
        &py_on_cluster_state_transaction,
        f, callable, _1));

    return f;
}


void make_context_bindings()
{
    bpl::class_<context, context::ptr_t, boost::noncopyable>(
            "Context", bpl::init<const spb::ClusterState &>())
        .def("get_server_uuid", &context::get_server_uuid,
            bpl::return_value_policy<bpl::copy_const_reference>())
        .def("get_server_hostname", &context::get_server_hostname,
            bpl::return_value_policy<bpl::copy_const_reference>())
        .def("get_server_port", &context::get_server_port)
        .def("get_cluster_state", &context::get_cluster_state,
            bpl::return_value_policy<bpl::copy_const_reference>())
        .def("cluster_state_transaction", &py_cluster_state_transaction);
}

}
}

