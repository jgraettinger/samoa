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
     *
     *  ClusterClock's has_pruned_clock field also requires explanation:
     *
     *    An issue with clock pruning, is it introduces edge cases where
     *    an record at least consistency_horizon old in the cluster exists,
     *    but a partition handling a write doesn't have it & creates a new
     *    record.
     *
     *    During a naive reconcilliation, elements have the potential to
     *    be dropped (eg, because a remote element is older than ignore_ts);
     *    it also introduces ambiguity in a datamodel's treatment of
     *    consistent value representations.
     *
     *    ClockUtil therefore introduces the notion of clock 'consistency',
     *    which means the clock is at least consistency_horizon old, and
     *    can therefore trace it's lineage to the initial write of the key.
     *
     *    A clock is considered 'consistent' if it has a partition clock
     *    at least ignore_ts old, or if has_pruned_clock is true.
     */

    static unsigned clock_jitter_bound /* = 3600 */;

    /// Returns whether the ClusterClock is well-formed
    static bool validate(const spb::ClusterClock &);

    /*! \brief Determines whether the clock should be considered consistent
     *
     * A clock is consistent if a PartitionClock is at least
     *  ignore_ts old, or if has_pruned_clock is true.
     */
    static bool is_consistent(const spb::ClusterClock &,
        unsigned consistency_horizon);

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

    /*! \brief Determines the relative ordering of two cluster clocks
     *
     * Clocks are equal iff they share identical partition clocks
     *  and clock consistency.
     *
     * A clock is more recent than the other iff it has all partition clocks
     *  of the other clock, where each clock is at least as-current, and any of:
     *    - it is consistent, and the other is not
     *    - one of it's partition clocks is newer
     *    - one of it's partition clocks isn't present in the other
     *
     * If clocks are unequal and neither is strictly newer than the other,
     *  then they are divergent.
     *
     * If the local clock is consistent, than any missing remote partition
     *  clocks older than prune_ts are assumed to have been pruned, and
     *  not to indicate a divergence.
     */
    static clock_ancestry compare(
        const spb::ClusterClock & local_clock,
        const spb::ClusterClock & remote_clock,
        unsigned consistency_horizon);

    /*! \brief Prunes elements of the clock which are older than prune_ts
     *
     * Additionally calls a DatamodelUpdate callable, where DatamodelUpdate
     *  is a concept with the signature void(unsigned index). A call signals
     *  the element at the specified index should be removed.
     *
     * If an element is pruned, has_pruned_clock is set.
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
     *  is a concept with the signature void(merge_step)
     *
     * The update functor is called for every partition clock in local_clock
     *  and remote_clock in strictly increasing partition-uuid order, and
     *  provides the datamodel's merge operation with relative ordering for
     *  partition elements.
     * In turn, the datamodel-specific functor is expected to maintain
     *  iterators into the local and remote partition elements, which are
     *  updated after each call following the semantics documented in the
     *  merge_step enum. See merge_step for details.
     *
     * If the local clock is consistent, than any missing remote partition
     *  clocks older than prune_ts are assumed to have been pruned, and
     *  not to indicate a divergence (RHS_SKIP is passed to the datamodel).
     *
     * If local_clock isn't consistent and remote_clock is, then
     *  local_was_updated will always be set, and local_clock will become
     *  consistent.
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

