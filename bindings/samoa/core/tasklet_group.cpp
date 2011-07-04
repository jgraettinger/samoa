
#include <boost/python.hpp>
#include "samoa/core/tasklet.hpp"
#include "samoa/core/tasklet_group.hpp"
#include "pysamoa/scoped_python.hpp"
#include "pysamoa/coroutine.hpp"

namespace samoa {
namespace core {

namespace bpl = boost::python;

void make_tasklet_group_bindings()
{
    bpl::class_<tasklet_group, tasklet_group::ptr_t, boost::noncopyable>(
        "TaskletGroup", bpl::init<>())
        .def("cancel_tasklet", &tasklet_group::cancel_tasklet)
        .def("cancel_group", &tasklet_group::cancel_group)
        .def("start_managed_tasklet", &tasklet_group::start_managed_tasklet)
        .def("start_orphaned_tasklet", &tasklet_group::start_orphaned_tasklet)
        ;
}

}
}

