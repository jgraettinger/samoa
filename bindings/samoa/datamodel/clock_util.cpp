
#include <boost/python.hpp>
#include "samoa/datamodel/clock_util.hpp"
#include <boost/uuid/uuid_io.hpp>
#include <sstream>

namespace samoa {
namespace datamodel {

namespace bpl = boost::python;

void py_tick(spb::ClusterClock & clock, uint64_t author_id,
    const bpl::object & py_update)
{
    clock_util::tick(clock, author_id, py_update);
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
        .def("generate_author_id", &clock_util::generate_author_id)
        .staticmethod("generate_author_id")
        ;

    bpl::enum_<clock_util::clock_ancestry>("ClockAncestry")
        .value("CLOCKS_EQUAL", clock_util::CLOCKS_EQUAL)
        .value("LOCAL_MORE_RECENT", clock_util::LOCAL_MORE_RECENT)
        .value("REMOTE_MORE_RECENT", clock_util::REMOTE_MORE_RECENT)
        .value("CLOCKS_DIVERGE", clock_util::CLOCKS_DIVERGE)
        ;

    bpl::enum_<clock_util::merge_step>("MergeStep")
        .value("LAUTH_RAUTH_EQUAL", clock_util::LAUTH_RAUTH_EQUAL)
        .value("RAUTH_PRUNED", clock_util::RAUTH_PRUNED)
        .value("LAUTH_PRUNED", clock_util::LAUTH_PRUNED)
        .value("RAUTH_ONLY", clock_util::RAUTH_ONLY)
        .value("LAUTH_ONLY", clock_util::LAUTH_ONLY)
        .value("RAUTH_NEWER", clock_util::RAUTH_NEWER)
        .value("LAUTH_NEWER", clock_util::LAUTH_NEWER)
        ;
}

}
}

