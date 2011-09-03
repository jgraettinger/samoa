
#include <boost/python.hpp>
#include "samoa/datamodel/clock_util.hpp"
#include <boost/uuid/uuid_io.hpp>
#include <sstream>

namespace samoa {
namespace datamodel {

namespace bpl = boost::python;

void make_clock_util_bindings()
{
    bpl::class_<clock_util>("ClockUtil", bpl::init<>())
        .def("tick", &clock_util::tick)
        .staticmethod("tick")
        .def("validate", &clock_util::validate)
        .staticmethod("validate")
        .def("prune_record", &clock_util::prune_record)
        .staticmethod("prune_record")
        .def("compare", &clock_util::compare,
            (bpl::arg("lhs"),
             bpl::arg("rhs"),
             bpl::arg("consistency_horizon") = 0,
             bpl::arg("merged_clock_out") = bpl::object()))
        .staticmethod("compare")
        .def_readwrite("clock_jitter_bound", &clock_util::clock_jitter_bound)
        ;

    bpl::enum_<clock_util::clock_ancestry>("ClockAncestry")
        .value("EQUAL", clock_util::EQUAL)
        .value("MORE_RECENT", clock_util::MORE_RECENT)
        .value("LESS_RECENT", clock_util::LESS_RECENT)
        .value("DIVERGE", clock_util::DIVERGE)
        ;
}

}
}

