#ifndef SAMOA_DATAMODEL_PARTITION_CLOCK_HPP
#define SAMOA_DATAMODEL_PARTITION_CLOCK_HPP

#include "samoa/core/uuid.hpp"
#include "samoa/core/server_time.hpp"
#include "samoa/datamodel/packed_unsigned.hpp"

namespace samoa {
namespace datamodel {

/*! \brief A logical timepoint from a partition's perspective

Strongly orders operations originating from a single partition. Each operation
is tagged with a unix timestamp and monotonically incrementing lamport tick.

Note that unix_timestamp has higher precedence. lamport_tick is still required
for sub-second resolution, but at larger resolutions lamport_tick may be restarted
at zero while still maintaining a total order.
*/
struct partition_clock
{
    core::uuid partition_uuid;
    uint64_t unix_timestamp;
    uint32_t lamport_tick;

    partition_clock(const core::uuid & u)
     : partition_uuid(u),
       unix_timestamp(0),
       lamport_tick(0)
    { }

    partition_clock()
     : partition_uuid(),
       unix_timestamp(0),
       lamport_tick(0)
    { }

    static unsigned serialized_length()
    {
        uint64_t timestamp = core::server_time::get_time();
        return  sizeof(core::uuid::data) +
                packed_unsigned::serialized_length(timestamp) +
                packed_unsigned::serialized_length(0);
    }

    /*! \brief Total order of partition_clock
    
        Orders partition_clock such that instances are grouped by
        partition_uuid (ascending), with instances appearing in descending
        order of recency (eg, most recent clock first).
    */
    struct max_comparator
    {
        bool operator()(const partition_clock &, const partition_clock &) const;
    };

    /*! \brief Orders partition_clock on uuid (only)
    
        Applied with std::unique() over a sequence previously ordered by
        max_comparator to remove all but the most recent partition_clocks
    */
    struct uuid_comparator
    {
        bool operator()(const partition_clock &, const partition_clock &) const;
        bool operator()(const partition_clock &, const core::uuid &) const;
        bool operator()(const core::uuid &, const partition_clock &) const;
    };

    bool operator == (const partition_clock & other) const;
};

inline bool partition_clock::max_comparator::operator()(
    const partition_clock & lhs, const partition_clock & rhs) const
{
    if(lhs.partition_uuid != rhs.partition_uuid)
        return lhs.partition_uuid < rhs.partition_uuid;

    if(lhs.unix_timestamp != rhs.unix_timestamp)
        return lhs.unix_timestamp > rhs.unix_timestamp;

    return lhs.lamport_tick > rhs.lamport_tick;
}

inline bool partition_clock::uuid_comparator::operator()(
    const partition_clock & lhs, const partition_clock & rhs) const
{
    return lhs.partition_uuid < rhs.partition_uuid;
}

inline bool partition_clock::uuid_comparator::operator()(
    const core::uuid & lhs, const partition_clock & rhs) const
{
    return lhs < rhs.partition_uuid;
}

inline bool partition_clock::uuid_comparator::operator()(
    const partition_clock & lhs, const core::uuid & rhs) const
{
    return lhs.partition_uuid < rhs;
}

inline bool partition_clock::operator == (const partition_clock & o) const
{
    return  partition_uuid == o.partition_uuid &&
            unix_timestamp == o.unix_timestamp &&
            lamport_tick == lamport_tick;
}

}
}

#endif

