#ifndef SAMOA_DATAMODEL_CLUSTER_CLOCK_HPP
#define SAMOA_DATAMODEL_CLUSTER_CLOCK_HPP

#include "samoa/core/protobuf/fwd.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include "samoa/core/uuid.hpp"
#include "samoa/datamodel/merge_func.hpp"
#include <string>

namespace samoa {
namespace datamodel {

namespace spb = samoa::core::protobuf;

class clock_util
{
public:

    /*!
     * consistency_horizon and clock_jitter_bound are composed
     * with the server's clock to produce two values:
     *
     *  - ignore_ts is defined as current-time - consistency_horizon
     *
     *    if a remote clock is missing a partition clock at least
     *    this old, it's assumed that the remote server pruned the
     *    clock, and the discrepancy is due to clock jitter
     *
     *  - prune_ts is defined as ignore_ts - clock_jitter_bound
     *
     *    if a partition clock is at least this old, it's considered
     *    prunable on the assumption that all other peers will see
     *    the clock as at least ignore_ts old
     */

    static unsigned clock_jitter_bound /* = 3600 */;

    /// Returns whether the ClusterClock is well-formed
    static bool validate(const spb::ClusterClock &);

    /*! \brief Ticks the clock of the named partition
     *
     * Additionally calls a DatamodelUpdate callable, where DatamodelUpdate
     *  is a concept with the following signature:
     *
     *  void(bool insert_before, unsigned index);
     *
     * If insert_before is true, a new datamodel element should be inserted
     *  before index to capture the mutation represented by this tick.
     * Otherwise, the datamodel element at index should be updated.
     */
    template<typename DatamodelUpdate>
    static void tick(spb::ClusterClock &,
        const core::uuid & partition_uuid,
        const DatamodelUpdate &);

    enum clock_ancestry
    {
        CLOCKS_EQUAL = 0,
        LOCAL_MORE_RECENT = 1,
        REMOTE_MORE_RECENT = 2,
        CLOCKS_DIVERGE = 3
    };

    /// Determines the relative ordering of two cluster clocks
    static clock_ancestry compare(
        const spb::ClusterClock & local_clock,
        const spb::ClusterClock & remote_clock,
        unsigned consistency_horizon);

    /*! \brief Prunes elements of the clock which are older than prune_ts
     *
     * Additionally calls a DatamodelUpdate callable, where DatamodelUpdate
     *  is a concept with the following signature, and signals the element
     *  at the specified index should be removed.
     *
     * void(unsigned index):
     */
    template<typename DatamodelUpdate>
    static void prune(spb::PersistedRecord &,
        unsigned consistency_horizon,
        const DatamodelUpdate &);

    enum merge_step
    {
        // local_clock is represented as the left-hand-side (LHS);
        //  remote_clock is the right-hand-side (RHS)
        //
        // documented with each enum value is the expected datamodel
        //  element iteration update

    	// lhs and rhs partition clocks are equal
    	//  update: lhs++, rhs++
        LHS_RHS_EQUAL = 0,

        // partition clock appears only on lhs
        //  update: lhs++
        LHS_ONLY = 1,

        // partition clock appearing only on rhs should be inserted before lhs
        //  update: lhs-insert-before, rhs++
        RHS_ONLY = 2,

        // lhs partition clock is newer than rhs
        //  update: lhs++, rhs++
        LHS_NEWER = 3,

        // rhs partition clock is newer than lhs
        //  update: lhs++, rhs++
        RHS_NEWER = 4,

        // lhs has already pruned partition clock appearing on rhs
        //  update: rhs++
        RHS_SKIP = 5
    };

    /*! \brief Merges remote_clock into local_clock
     *
     * Given (possibly divergent) local_clock & remote_clock, remote_clock is
     *  folded into local_clock, such that local_clock post-operation is equal
     *  or more recent than both pre-operation local_clock and remote_clock.
     *
     * Additionally calls a DatamodelUpdate callable, where DatamodelUpdate
     *  is a concept with the following signature:
     *
     * void(merge_step);
     *
     * The update functor is called for every partition clock in local_clock
     *  and remote_clock in strictly increasing partition-uuid order, and
     *  provides the datamodel's merge operation with relative ordering for
     *  partition elements.
     * In turn, the datamodel-specific functor is expected to maintain
     *  iterators into the local and remote partition elements, which are
     *  updated after each call following the semantics documented in the
     *  merge_step enum. See merge_step for details.
     */
    template<typename DatamodelUpdate>
    static merge_result merge(
        spb::ClusterClock & local_clock,
        const spb::ClusterClock & remote_clock,
        unsigned consistency_horizon,
        const DatamodelUpdate &);

private:

    typedef google::protobuf::RepeatedPtrField<spb::PartitionClock> clocks_t;
};

}
}

#include "samoa/datamodel/clock_util.impl.hpp"

#endif

