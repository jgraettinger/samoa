#ifndef SAMOA_DATAMODEL_CLUSTER_CLOCK_HPP
#define SAMOA_DATAMODEL_CLUSTER_CLOCK_HPP

#include "samoa/core/protobuf/fwd.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include "samoa/core/uuid.hpp"
#include <string>

namespace samoa {
namespace datamodel {

namespace spb = samoa::core::protobuf;

class clock_util
{
public:

    static void tick(spb::ClusterClock &, const core::uuid & partition_uuid);

    static bool validate(const spb::ClusterClock &);

    static void prune_record(const spb::PersistedRecord_ptr_t &,
        unsigned consistency_horizon);

    enum clock_ancestry
    {
        EQUAL = 0,
        MORE_RECENT = 1,
        LESS_RECENT = 2,
        DIVERGE = 3
    };

    /*!
    \brief Compares two cluster clocks, optionally merging to a third

    If consistency_horizon is non-zero, then:
        * ignore_ts is defined as current-server-time - horizon
        * prune_ts is defined as ignore_ts - clock_jitter_bound

    Records prune_ts or older are not factored into the ancestry
    result, and are not included in the merged clock.

    Records newer than prune_ts but older than ignore_ts are not
    factored into the ancestry result, but _are_ included in the
    merged clock.

    This behavior is designed to handle jitter from network delay
    and clock sync issues across nodes in the cluster. Conceptually,
    all nodes agree that after consistency_horizon a record shouldn't
    be used in ancestry computations, but the record can't actually
    be dropped until we're confident all nodes can agree that the
    record has passed the consistency_horizon threshold.

    consistency_horizon of zero is treated as if there's an infinite
    horizon (records are never ignored or pruned).

    If non-null, merged_clock_out is cleared and assigned the merged
    clock value.
    */
    static clock_ancestry compare(
        const spb::ClusterClock & lhs,
        const spb::ClusterClock & rhs,
        unsigned consistency_horizon = 0,
        spb::ClusterClock * merged_clock_out = 0);

    static unsigned clock_jitter_bound;
};

}
}

#endif

