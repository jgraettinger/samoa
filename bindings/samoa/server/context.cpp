#include "samoa/server/context.hpp"
#include "samoa/client/server_pool.hpp"
#include <boost/python.hpp>

namespace samoa {
namespace server {

using namespace boost::python;

void make_context_bindings()
{
    class_<context, context::ptr_t, boost::noncopyable>(
        "Context", init<core::proactor::ptr_t>(args("proactor")))
        .def("get_proactor", &context::get_proactor,
            return_value_policy<copy_const_reference>())
        .def("get_peer_pool", &context::get_peer_pool,
            return_value_policy<copy_const_reference>());
}

}
}

