
#include <boost/python.hpp>
#include "samoa/datamodel/merge_func.hpp"

namespace samoa {
namespace datamodel {

namespace bpl = boost::python;

void make_merge_func_bindings()
{
    bpl::class_<merge_result>("MergeResult", bpl::init<>())
        .def(bpl::init<bool, bool>((
            bpl::arg("local_was_updated"),
            bpl::arg("remote_is_stale"))))
        .def_readwrite("local_was_updated",
            &merge_result::local_was_updated)
        .def_readwrite("remote_is_stale",
            &merge_result::remote_is_stale);
}

}
}

