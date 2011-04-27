
#include <boost/python.hpp>
#include "samoa/server/context.hpp"
#include "samoa/client/server_pool.hpp"

namespace samoa {
namespace server {

namespace bpl = boost::python;

void make_context_bindings()
{
    bpl::class_<context, context::ptr_t, boost::noncopyable>(
        "Context", bpl::init<core::proactor::ptr_t>(
            (bpl::arg("proactor"))))
        .def("get_proactor", &context::get_proactor,
            bpl::return_value_policy<bpl::copy_const_reference>());
}

}
}

