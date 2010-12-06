#include "samoa/server/context.hpp"
#include <boost/python.hpp>

namespace server {

using namespace boost::python;

void make_context_bindings()
{
    class_<context, context::ptr_t, boost::noncopyable>(
        "context", init<core::proactor::ptr_t>())
        .add_property("proactor", make_function(&context::get_proactor,
            return_value_policy<copy_const_reference>()));
}

}

