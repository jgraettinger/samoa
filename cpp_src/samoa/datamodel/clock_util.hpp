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
     *    if a remote clock is missing an author clock at least
     *    this old, it's assumed that the remote server pruned the
     *    clock, and the discrepancy is due to clock jitter
     *
     *  - prune_ts is defined as ignore_ts - clock_jitter_bound
     *
     *    if a author clock is at least this old, it's considered
     *    prunable on the assumption that all other peers will see
     *    the clock as at least ignore_ts old
     *
     * On 'legacy' clocks:
     *
     *    An issue with clock pruning is it introduces edge cases where
     *    a record at least consistency_horizon old in the cluster exists,
     *    but a partition handling a write doesn't have it and creates a
     *    new record.
     *
     *    During a naive reconcilliation, elements have the potential to
     *    be dropped (eg, because a remote author is older than prune_ts);
     *    we may also assume the remote is up to date, as we assume it's
     *    pruned an element ignore_ts old. Datamodel updates are made
     *    similarly ambiguous.
     *
     *    ClockUtil therefore introduces the notion of a 'legacy' clock,
     *    which means the clock is at least consistency_horizon old, and
     *    can trace it's lineage to the initial write of the key.
     *
     *    A clock (local or remote) is considered 'legacy' if an author
     *    clock has been pruned (tracked by the 'clock_is_pruned' boolean of
     *    ClusterClock), or if an AuthorClock is at least ignore_ts old.
     *
     *    A legacy clock never compare()'s as EQUAL to a non-legacy clock;
     *    it's either more-recent or divergent. Likewise in merge_result
     *    parlance, a legacy remote clock always updates a non-legacy local,
     *    and a non-legacy remote is always stale vs a legacy local.
     *
     *    A 'legacy merge' is a merge between a legacy remote clock,
     *    and a non-legacy local clock. As a result of a legacy merge, the
     *    local clock always itself becomes 'legacy'. The datamodel is
     *    required to manage inheritance of any non-versioned legacy content.
     */

    static unsigned clock_jitter_bound /* = 3600 */;

    /*! \brief Generates a new, random author id
     *
     * IDs are generated using mersenne twister, and seeded with system entropy
     */
    static uint64_t generate_author_id();

    /// Returns whether the ClusterClock is well-formed
    static bool validate(const spb::ClusterClock &);

    /*! \brief Ticks the clock of the identified author
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
    static void tick(spb::ClusterClock &, uint64_t author_id,
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
     * Clocks are equal if they share identical author clocks and legacy status
     *
     * A clock is more recent than the other iff it has all author clocks
     *  of the other clock, where each clock is at least as-current, and:
     *    - it is legacy and the other is not, or
     *    - one of it's author clocks is newer, or
     *    - one of it's author clocks isn't present in the other
     *
     * If clocks are unequal and neither is strictly newer than the other,
     *  then they are divergent.
     *
     * If the local clock is legacy, than any missing remote author
     *  clocks older than prune_ts are assumed to have been pruned,
     *  and not indicative of divergence.
     */
    static clock_ancestry compare(
        const spb::ClusterClock & local_clock,
        const spb::ClusterClock & remote_clock,
        unsigned consistency_horizon);

    /*! \brief Prunes author clocks of the cluster clock older than prune_ts
     *
     * Additionally calls a DatamodelUpdate callable, where DatamodelUpdate
     *  is a concept with the signature void(unsigned index). A call signals
     *  the element at the specified index should be removed.
     *
     * If an element is pruned, clock_is_pruned is set.
     */
    template<typename DatamodelUpdate>
    static void prune(spb::PersistedRecord &, unsigned consistency_horizon,
        const DatamodelUpdate &);

    enum merge_step
    {
        // enumerates possible states, when comparing local & remote
        //  author clocks for purposes of merging. documented with each enum
        //  value is the expected datamodel local & remote iterator update

        // local and remote author clocks are equal
        //  update: lauth++, rauth++
        LAUTH_RAUTH_EQUAL,

        // assumed that remote clock has pruned local's author clock
        //  update: lauth++
        RAUTH_PRUNED,

        // assumed that local clock has pruned remote's author clock
        //  update: rauth++
        LAUTH_PRUNED,

        // new author appears only in remote clock, and will be
        //      inserted before the local author clock iterator
        //  update: insert-before-lauth, rauth++
        RAUTH_ONLY,

        // new author appears only in local clock
        //  update: lauth++
        LAUTH_ONLY,

        // remote author clock is newer than local
        //  update: lauth++, rauth++
        RAUTH_NEWER,

        // local author clock is newer than remote
        //  update: lauth++, rauth++
        LAUTH_NEWER
    };

    /*! \brief Merges remote_clock into local_clock
     *
     * Given (possibly divergent) local_clock & remote_clock, remote_clock is
     *  folded into local_clock, such that local_clock post-operation is equal
     *  or more recent than both pre-operation local_clock and remote_clock.
     *
     * Additionally calls a DatamodelUpdate callable, where DatamodelUpdate
     *  is a concept with the signature:
     *
     *      void(bool local_is_legacy, bool remote_is_legacy, merge_step)
     *
     * The update functor is called for every partition clock in local_clock
     *  and remote_clock in strictly increasing author_id order, and
     *  provides the datamodel's merge operation with relative ordering for
     *  author clocks.
     *
     * In turn, the datamodel-specific functor is expected to maintain
     *  iterators into the local and remote author values, which are
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

    typedef google::protobuf::RepeatedPtrField<
        spb::AuthorClock> author_clocks_t;

    static bool has_timestamp_reached(const spb::ClusterClock &, uint64_t);
};

}
}

#include "samoa/datamodel/clock_util.impl.hpp"

#endif

