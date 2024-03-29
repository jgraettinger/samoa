
#include "samoa/datamodel/clock_util.hpp"
#include "samoa/core/server_time.hpp"
#include "samoa/error.hpp"

namespace samoa {
namespace datamodel {

namespace spb = samoa::core::protobuf;

// default jitter bound is 1 hour
unsigned clock_util::clock_jitter_bound = 3600;

struct uuid_comparator
{
    bool operator()(const spb::PartitionClock &, const spb::PartitionClock &) const;
    bool operator()(const spb::PartitionClock &, const std::string &) const;
    bool operator()(const std::string &, const spb::PartitionClock &) const;
};

inline bool uuid_comparator::operator()(
    const spb::PartitionClock & lhs, const spb::PartitionClock & rhs) const
{
    return lhs.partition_uuid() < rhs.partition_uuid();
}

inline bool uuid_comparator::operator()(
    const spb::PartitionClock & lhs, const std::string & rhs) const
{
    return lhs.partition_uuid() < rhs;
}

inline bool uuid_comparator::operator()(
    const std::string & lhs, const spb::PartitionClock & rhs) const
{
    return lhs < rhs.partition_uuid();
}

void clock_util::tick(spb::ClusterClock & cluster_clock,
    const core::uuid & partition_uuid)
{
    typedef google::protobuf::RepeatedPtrField<spb::PartitionClock> clocks_t;
    clocks_t & clocks = *cluster_clock.mutable_partition_clock();

    std::string p_uuid_bytes(partition_uuid.begin(), partition_uuid.end());

    clocks_t::iterator it = std::lower_bound(clocks.begin(), clocks.end(),
        p_uuid_bytes, uuid_comparator());

    // no current clock for this partition? add one
    if(it == clocks.end() || it->partition_uuid() != p_uuid_bytes)
    {
        int index = std::distance(clocks.begin(), it);

        spb::PartitionClock * new_clock = clocks.Add();

        new_clock->mutable_partition_uuid()->swap(p_uuid_bytes);

        // bubble new_clock to appear at index,
        //  re-establishing sorted order on uuid
        for(int j = clocks.size(); --j != index;)
        {
            clocks.SwapElements(j, j - 1);
        }

        it = clocks.begin() + index;
    }

    it->set_unix_timestamp(core::server_time::get_time());
    it->set_lamport_tick(it->lamport_tick() + 1);
}

bool clock_util::validate(const spb::ClusterClock & cluster_clock)
{
    typedef google::protobuf::RepeatedPtrField<spb::PartitionClock> clocks_t;
    const clocks_t & clocks = cluster_clock.partition_clock();

    clocks_t::const_iterator it = clocks.begin();
    clocks_t::const_iterator last_it = it++;

    while(last_it != clocks.end() && it != clocks.end())
    {
        if(!uuid_comparator()(*last_it, *it))
        {
            return false;
        }
        last_it = it++;
    }
    return true;
}

void clock_util::prune_record(spb::PersistedRecord & record,
    unsigned consistency_horizon)
{
    // records are pruned if they're older than horizon _plus_ jitter bound
    uint64_t ignore_ts = core::server_time::get_time() - consistency_horizon;
    uint64_t prune_ts = ignore_ts - clock_jitter_bound;

    typedef google::protobuf::RepeatedPtrField<spb::PartitionClock> clocks_t;
    spb::ClusterClock & cluster_clock = *record.mutable_cluster_clock();
    clocks_t & clocks = *cluster_clock.mutable_partition_clock();

    clocks_t::iterator it = clocks.begin();
    clocks_t::iterator last_it = it;

    while(it != clocks.end())
    {
        SAMOA_ASSERT(last_it == it || uuid_comparator()(*last_it, *it));

        if(it->unix_timestamp() <= prune_ts)
        {
            // bubble record to vector tail, and remove
            int index = std::distance(clocks.begin(), it);

            for(int i = index; (i + 1) != clocks.size(); ++i)
            {
                clocks.SwapElements(i, i + 1);
            }
            clocks.RemoveLast();

            // reset iterators
            last_it = it = clocks.begin() + index;
        }
        else
            last_it = it++;
    }

    if(clocks.size() == 0)
        record.clear_cluster_clock();
}

clock_util::clock_ancestry clock_util::compare(
    const spb::ClusterClock & lhs_clock,
    const spb::ClusterClock & rhs_clock,
    unsigned consistency_horizon,
    spb::ClusterClock * merged_clock_out)
{
    uint64_t ignore_ts = 0, prune_ts = 0;

    if(consistency_horizon)
    {
        // records older than horizon delta _plus_ jitter bound are pruned
        ignore_ts = core::server_time::get_time() - consistency_horizon;
        prune_ts = ignore_ts - clock_jitter_bound;
    }

    typedef google::protobuf::RepeatedPtrField<spb::PartitionClock> clocks_t;
    const clocks_t & lhs = lhs_clock.partition_clock();
    const clocks_t & rhs = rhs_clock.partition_clock();

    clocks_t * merged = 0;
    if(merged_clock_out)
    {
        merged = merged_clock_out->mutable_partition_clock();
        merged->Clear();
    }

    clocks_t::const_iterator l_it = lhs.begin();
    clocks_t::const_iterator r_it = rhs.begin();

    bool lhs_more_recent = false;
    bool rhs_more_recent = false;

    while(l_it != lhs.end() && r_it != rhs.end())
    {
        if(l_it->partition_uuid() < r_it->partition_uuid())
        {
            if(l_it->unix_timestamp() > ignore_ts)
                lhs_more_recent = true;

            if(merged && l_it->unix_timestamp() > prune_ts)
                merged->Add()->CopyFrom(*l_it);

            ++l_it;
        }
        else if(l_it->partition_uuid() > r_it->partition_uuid())
        {
            if(r_it->unix_timestamp() > ignore_ts)
                rhs_more_recent = true;

            if(merged && r_it->unix_timestamp() > prune_ts)
                merged->Add()->CopyFrom(*r_it);

            ++r_it;
        }
        else
        {
            // partition_uuid's are equal:
            //  compare timestamps and lamport ticks to determine ordering

            if(l_it->unix_timestamp() <= prune_ts && \
               r_it->unix_timestamp() <= prune_ts)
            {
                // timestamps are too old => ignore
            }

            // at least one timestamp is recent enough to keep the record
            else if(l_it->unix_timestamp() < r_it->unix_timestamp())
            {
                if(r_it->unix_timestamp() > ignore_ts)
                    rhs_more_recent = true;

                if(merged)
                    merged->Add()->CopyFrom(*r_it);
            }
            else if(l_it->unix_timestamp() > r_it->unix_timestamp())
            {
                if(l_it->unix_timestamp() > ignore_ts)
                    lhs_more_recent = true;

                if(merged)
                    merged->Add()->CopyFrom(*l_it);
            }

            // timestamps are the same; next is lamport ticks
            else if(l_it->lamport_tick() < r_it->lamport_tick())
            {
                if(r_it->unix_timestamp() > ignore_ts)
                    rhs_more_recent = true;

                if(merged)
                    merged->Add()->CopyFrom(*r_it);
            }
            else if(l_it->lamport_tick() > r_it->lamport_tick())
            {
                if(l_it->unix_timestamp() > ignore_ts)
                    lhs_more_recent = true;

                if(merged)
                    merged->Add()->CopyFrom(*l_it);
            }

            // partition clocks are identical
            else if(merged)
            {
                merged->Add()->CopyFrom(*l_it);
            }

            ++l_it; ++r_it;
        }
    }
    while(l_it != lhs.end())
    {
        if(l_it->unix_timestamp() > prune_ts)
        {
            if(l_it->unix_timestamp() > ignore_ts)
                lhs_more_recent = true;

            if(merged)
                merged->Add()->CopyFrom(*l_it);
        }
        ++l_it;            
    }
    while(r_it != rhs.end())
    {
        if(r_it->unix_timestamp() > prune_ts)
        {
            if(r_it->unix_timestamp() > ignore_ts)
                rhs_more_recent = true;

            if(merged)
                merged->Add()->CopyFrom(*r_it);
        }
        ++r_it;
    }

    if(lhs_more_recent && rhs_more_recent)
        return DIVERGE;
    if(lhs_more_recent)
        return MORE_RECENT;
    if(rhs_more_recent)
        return LESS_RECENT;

    return EQUAL;
}

}
}

