
#include "samoa/core/protobuf/sequence_util.hpp"
#include "samoa/core/server_time.hpp"
#include "samoa/log.hpp"

namespace samoa {
namespace datamodel {

namespace spb = samoa::core::protobuf;

using std::begin;
using std::end;

struct uuid_comparator
{
    bool operator()(const spb::PartitionClock &,
        const spb::PartitionClock &) const;
    bool operator()(const spb::PartitionClock &, const core::uuid &) const;
    bool operator()(const core::uuid &, const spb::PartitionClock &) const;
};

inline bool uuid_comparator::operator()(
    const spb::PartitionClock & lhs, const spb::PartitionClock & rhs) const
{
    return lhs.partition_uuid() < rhs.partition_uuid();
}

inline bool uuid_comparator::operator()(
    const spb::PartitionClock & lhs, const core::uuid & rhs) const
{
    // reinterpret as unsigned
    const uint8_t * lhs_uuid = reinterpret_cast<const uint8_t*>(
        lhs.partition_uuid().data());

    return std::lexicographical_compare(
        lhs_uuid, lhs_uuid + lhs.partition_uuid().size(),
        begin(rhs), end(rhs));
}

inline bool uuid_comparator::operator()(
    const core::uuid & lhs, const spb::PartitionClock & rhs) const
{
    // reinterpret as unsigned
    const uint8_t * rhs_uuid = reinterpret_cast<const uint8_t*>(
        rhs.partition_uuid().data());

    return std::lexicographical_compare(
        begin(lhs), end(lhs),
        rhs_uuid, rhs_uuid + rhs.partition_uuid().size());
}

template<typename DatamodelUpdate>
void clock_util::tick(spb::ClusterClock & cluster_clock,
    const core::uuid & partition_uuid,
    const DatamodelUpdate & datamodel_update)
{
    clocks_t & clocks = *cluster_clock.mutable_partition_clock();
    clocks_t::iterator it = std::lower_bound(clocks.begin(), clocks.end(),
        partition_uuid, uuid_comparator());

    bool is_equal = false;
    if(it != end(clocks))
    {
        SAMOA_ASSERT(it->partition_uuid().size() == sizeof(core::uuid));

        // reinterpret string uuid as unsigned
        is_equal = std::equal(begin(partition_uuid), end(partition_uuid),
            reinterpret_cast<const uint8_t*>(it->partition_uuid().data()));
    }

    size_t ind = std::distance(clocks.begin(), it);

    if(!is_equal)
    {
        // no current clock for this partition; add one
        spb::PartitionClock & new_clock = insert_before(clocks, it);
        new_clock.mutable_partition_uuid()->assign(
            begin(partition_uuid), end(partition_uuid));

        new_clock.set_unix_timestamp(core::server_time::get_time());
        datamodel_update(true, ind);
    }
    else
    {
        if(it->unix_timestamp() < core::server_time::get_time())
        {
            it->set_unix_timestamp(core::server_time::get_time());
            it->clear_lamport_tick();
        }
        else
            it->set_lamport_tick(it->lamport_tick() + 1);

        datamodel_update(false, ind);
    }
}

template<typename DatamodelUpdate>
void clock_util::prune(spb::PersistedRecord & record,
    unsigned consistency_horizon,
    const DatamodelUpdate & datamodel_update)
{
    uint64_t ignore_ts = core::server_time::get_time() - consistency_horizon;
    uint64_t prune_ts = ignore_ts - clock_jitter_bound;

    spb::ClusterClock & cluster_clock = *record.mutable_cluster_clock();
    clocks_t & clocks = *cluster_clock.mutable_partition_clock();

    clocks_t::iterator it = clocks.begin();
    clocks_t::iterator last_it = it;

    while(it != end(clocks))
    {
        SAMOA_ASSERT(last_it == it || uuid_comparator()(*last_it, *it));

        if(it->unix_timestamp() <= prune_ts)
        {
            datamodel_update(std::distance(clocks.begin(), it));

            core::protobuf::remove_before(clocks, ++it);
            last_it = (it == begin(clocks)) ? it : it - 1;

            if(cluster_clock.has_clock_is_pruned())
            {
                cluster_clock.clear_clock_is_pruned();
            }
        }
        else
            last_it = it++;
    }
    if(clocks.size() == 0 && cluster_clock.clock_is_pruned())
    {
        record.clear_cluster_clock();
    }
}

template<typename DatamodelUpdate>
merge_result clock_util::merge(
    spb::ClusterClock & local_cluster_clock,
    const spb::ClusterClock & remote_cluster_clock,
    unsigned consistency_horizon,
    const DatamodelUpdate & datamodel_update)
{
    uint64_t ignore_ts = core::server_time::get_time() - consistency_horizon;
    uint64_t prune_ts = ignore_ts - clock_jitter_bound;

    clocks_t & local = *local_cluster_clock.mutable_partition_clock();
    const clocks_t & remote = remote_cluster_clock.partition_clock();

    clocks_t::iterator l_it = begin(local);
    clocks_t::const_iterator r_it = begin(remote);

    merge_result result = {false, false};

    bool local_is_consistent = is_consistent(local_cluster_clock,
        consistency_horizon);
    bool remote_is_consistent = is_consistent(remote_cluster_clock,
        consistency_horizon);

    if(local_is_consistent && !remote_is_consistent)
        result.remote_is_stale = true;

    while(l_it != local.end() && r_it != remote.end())
    {
        if(l_it->partition_uuid() < r_it->partition_uuid())
        {
            if(l_it->unix_timestamp() > ignore_ts)
                result.remote_is_stale = true;

            datamodel_update(LHS_ONLY);
            ++l_it;
        }
        else if(l_it->partition_uuid() > r_it->partition_uuid())
        {
            if(!local_is_consistent || r_it->unix_timestamp() > prune_ts)
            {
                result.local_was_updated = true;

                core::protobuf::insert_before(local, l_it) = *r_it;
                datamodel_update(RHS_ONLY);
            }
            else
            {
                datamodel_update(RHS_SKIP);
            }
            ++r_it;
        }
        else
        {
            if(l_it->unix_timestamp() > r_it->unix_timestamp())
            {
                result.remote_is_stale = true;
                datamodel_update(LHS_NEWER);
            }
            else if(l_it->unix_timestamp() < r_it->unix_timestamp())
            {
                l_it->set_unix_timestamp(r_it->unix_timestamp());
                l_it->set_lamport_tick(r_it->lamport_tick());

                result.local_was_updated = true;
                datamodel_update(RHS_NEWER);
            }
            else
            {
                if(l_it->lamport_tick() > r_it->lamport_tick())
                {
                    result.remote_is_stale = true;
                    datamodel_update(LHS_NEWER);
                }
                else if(l_it->lamport_tick() < r_it->lamport_tick())
                {
                    l_it->set_lamport_tick(r_it->lamport_tick());

                    result.local_was_updated = true;
                    datamodel_update(RHS_NEWER);
                }
                else
                {
                    datamodel_update(LHS_RHS_EQUAL);
                }
            }
            ++l_it; ++r_it;
        }
    }
    while(l_it != local.end())
    {
        if(l_it->unix_timestamp() > ignore_ts)
            result.remote_is_stale = true;

        datamodel_update(LHS_ONLY);
        ++l_it;
    }
    while(r_it != remote.end())
    {
        if(!local_is_consistent || r_it->unix_timestamp() > prune_ts)
        {
            result.local_was_updated = true;

            core::protobuf::insert_before(local, l_it) = *r_it;
            datamodel_update(RHS_ONLY);
        }
        else
        {
            datamodel_update(RHS_SKIP);
        }
        ++r_it;
    }

    if(!local_cluster_clock.clock_is_pruned() && \
        remote_cluster_clock.clock_is_pruned())
    {
        result.local_was_updated = true;
        local_cluster_clock.clear_clock_is_pruned();
    }
    return result;
}

}
}

