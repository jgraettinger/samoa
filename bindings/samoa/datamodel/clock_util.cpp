
#include <boost/python.hpp>
#include "samoa/datamodel/clock_util.hpp"
#include <boost/uuid/uuid_io.hpp>
#include <sstream>

namespace samoa {
namespace datamodel {

namespace bpl = boost::python;

void py_tick(spb::ClusterClock & clock,
    const core::uuid & partition_uuid,
    const bpl::object & py_update)
{
    clock_util::tick(clock, partition_uuid, py_update);
}

void py_prune(spb::PersistedRecord & record,
    unsigned consistency_horizon,
    const bpl::object & py_update)
{
    clock_util::prune(record, consistency_horizon, py_update);
}

merge_result py_merge(spb::ClusterClock & local_clock,
    const spb::ClusterClock & remote_clock,
    unsigned consistency_horizon,
    const bpl::object & py_update)
{
    return clock_util::merge(local_clock, remote_clock,
        consistency_horizon, py_update);
}

void make_clock_util_bindings()
{
    bpl::class_<clock_util>("ClockUtil", bpl::init<>())
        .def_readwrite("clock_jitter_bound", &clock_util::clock_jitter_bound)
        .def("validate", &clock_util::validate).staticmethod("validate")
        .def("tick", &py_tick).staticmethod("tick")
        .def("compare", &clock_util::compare).staticmethod("compare")
        .def("prune", &py_prune).staticmethod("prune")
        .def("merge", &py_merge).staticmethod("merge")
        ;

    bpl::enum_<clock_util::clock_ancestry>("ClockAncestry")
        .value("CLOCKS_EQUAL", clock_util::CLOCKS_EQUAL)
        .value("LOCAL_MORE_RECENT", clock_util::LOCAL_MORE_RECENT)
        .value("REMOTE_MORE_RECENT", clock_util::REMOTE_MORE_RECENT)
        .value("CLOCKS_DIVERGE", clock_util::CLOCKS_DIVERGE)
        ;

    bpl::enum_<clock_util::merge_step>("MergeStep")
        .value("LHS_RHS_EQUAL", clock_util::LHS_RHS_EQUAL)
        .value("LHS_ONLY", clock_util::LHS_ONLY)
        .value("RHS_ONLY", clock_util::RHS_ONLY)
        .value("LHS_NEWER", clock_util::LHS_NEWER)
        .value("RHS_NEWER", clock_util::RHS_NEWER)
        .value("RHS_SKIP", clock_util::RHS_SKIP)
        ;
}

}
}

