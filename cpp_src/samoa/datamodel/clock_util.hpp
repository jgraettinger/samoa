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
     * Additionally calls a DatamodelUpdate callable,
     *  where DatamodelUpdate is a concept with the
     *  following signature:
     *
     *  void(bool insert_before, unsigned index);
     *
     * If insert_before is true, a new datamodel element
     *  should be inserted before index to capture the
     *  mutation represented by this tick.
     * Otherwise, the datamodel element at index should
     *  be updated.
     */
    template<typename DatamodelUpdate>
    static void tick(spb::ClusterClock &,
        const core::uuid & partition_uuid,
        const DatamodelUpdate &);

    /*! \brief tick() variant without a DatamodelUpdate
     *
     * This variant is exposed principally for functional
     *  unit-testing. Ordinarily, ticks always accompany
     *  datamodel mutations.
     */
    static void tick(spb::ClusterClock &,
        const core::uuid & partition_uuid);

    enum clock_ancestry
    {
        CLOCKS_EQUAL = 0,
        LOCAL_MORE_RECENT = 1,
        REMOTE_MORE_RECENT = 2,
        CLOCKS_DIVERGE = 3
    };

    /*! \brief Determines the relative ordering of two cluster clocks
     *

    If non-null, merged_clock_out is cleared and assigned the merged
    clock value.
    */
    static clock_ancestry compare(
        const spb::ClusterClock & local_clock,
        const spb::ClusterClock & remote_clock,
        unsigned consistency_horizon);

    template<typename DatamodelUpdate>
    static void prune(spb::PersistedRecord &,
        unsigned consistency_horizon,
        const DatamodelUpdate &);

    enum merge_compare
    {
    	// lhs and rhs partition clocks are equal
    	//  update: lhs++, rhs++
        LHS_RHS_EQUAL = 0,

        // partition clock appears only on lhs
        //  update: lhs++
        LHS_ONLY = 1,

        // partition clock only on rhs should be inserted before lhs
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

