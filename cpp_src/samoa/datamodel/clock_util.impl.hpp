
#include "samoa/core/protobuf/sequence_util.hpp"
#include "samoa/core/server_time.hpp"

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
    return std::lexicographical_compare(
        begin(lhs.partition_uuid()), end(lhs.partition_uuid()),
        begin(rhs), end(rhs));
}

inline bool uuid_comparator::operator()(
    const core::uuid & lhs, const spb::PartitionClock & rhs) const
{
    return std::lexicographical_compare(
        begin(lhs), end(lhs),
        begin(rhs.partition_uuid()), end(rhs.partition_uuid()));
}

template<typename DatamodelUpdate>
void clock_util::tick(spb::ClusterClock & cluster_clock,
    const core::uuid & partition_uuid,
    const DatamodelUpdate & datamodel_update)
{
    clocks_t & clocks = *cluster_clock.mutable_partition_clock();
    clocks_t::iterator it = std::lower_bound(clocks.begin(), clocks.end(),
        partition_uuid, uuid_comparator());

    size_t ind = std::distance(clocks.begin(), it);

    SAMOA_ASSERT(it == clocks.end() || \
        it->partition_uuid().size() == sizeof(core::uuid));

    // no current clock for this partition? add one
    if(it == clocks.end() || std::equal(
    	begin(it->partition_uuid()), end(it->partition_uuid()),
    	begin(partition_uuid)))
    {
        spb::PartitionClock & new_clock = insert_before(clocks, it);
        new_clock.mutable_partition_uuid()->assign(
            begin(partition_uuid), end(partition_uuid));

        datamodel_update(true, ind);
    }
    else
        datamodel_update(false, ind);

    it->set_unix_timestamp(core::server_time::get_time());
    it->set_lamport_tick(it->lamport_tick() + 1);
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

    while(it != clocks.end())
    {
        SAMOA_ASSERT(last_it == it || uuid_comparator()(*last_it, *it));

        if(it->unix_timestamp() < prune_ts)
        {
        	datamodel_update(std::distance(clocks.begin(), it));

            ++it;
            core::protobuf::remove_before(clocks, it);
        }
        else
        	last_it = it++;
    }

    if(clocks.size() == 0)
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
            if(r_it->unix_timestamp() > prune_ts)
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
        if(r_it->unix_timestamp() > prune_ts)
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
    return result;
}

}
}

