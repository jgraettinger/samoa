
#include "samoa/datamodel/clock_util.hpp"
#include "samoa/core/server_time.hpp"
#include "samoa/error.hpp"

namespace samoa {
namespace datamodel {

namespace spb = samoa::core::protobuf;

// default jitter bound is 1 hour
unsigned clock_util::clock_jitter_bound = 3600;

bool clock_util::validate(const spb::ClusterClock & cluster_clock)
{
    typedef google::protobuf::RepeatedPtrField<spb::PartitionClock> clocks_t;
    const clocks_t & clocks = cluster_clock.partition_clock();

    clocks_t::const_iterator it = begin(clocks);
    clocks_t::const_iterator last_it = it++;

    while(last_it != clocks.end() && it != clocks.end())
    {
    	if(it->partition_uuid().size() != sizeof(core::uuid))
        {
    		return false;
        }
        if(!uuid_comparator()(*last_it, *it))
        {
            return false;
        }
        last_it = it++;
    }
    return true;
}

clock_util::clock_ancestry clock_util::compare(
    const spb::ClusterClock & local_clock,
    const spb::ClusterClock & remote_clock,
    unsigned consistency_horizon)
{
    uint64_t ignore_ts = core::server_time::get_time() - consistency_horizon;
    uint64_t prune_ts = ignore_ts - clock_jitter_bound;

    const clocks_t & local = local_clock.partition_clock();
    const clocks_t & remote = remote_clock.partition_clock();

    clocks_t::const_iterator l_it = begin(local);
    clocks_t::const_iterator r_it = begin(remote);

    bool local_more_recent = false;
    bool remote_more_recent = false;

    while(l_it != local.end() && r_it != remote.end())
    {
        if(l_it->partition_uuid() < r_it->partition_uuid())
        {
            if(l_it->unix_timestamp() > ignore_ts)
                local_more_recent = true;

            ++l_it;
        }
        else if(l_it->partition_uuid() > r_it->partition_uuid())
        {
            if(r_it->unix_timestamp() > prune_ts)
                remote_more_recent = true;

            ++r_it;
        }
        else
        {
            if(l_it->unix_timestamp() > r_it->unix_timestamp())
            {
                local_more_recent = true;
            }
            else if(l_it->unix_timestamp() < r_it->unix_timestamp())
            {
            	remote_more_recent = true;
            }
            else
            {
                if(l_it->lamport_tick() > r_it->lamport_tick())
                {
                	local_more_recent = true;
                }
                else if(l_it->lamport_tick() < r_it->lamport_tick())
                {
                	remote_more_recent = true;
                }
            }
            ++l_it; ++r_it;
        }
    }
    while(l_it != local.end())
    {
        if(l_it->unix_timestamp() > ignore_ts)
            local_more_recent = true;

        ++l_it;
    }
    while(r_it != remote.end())
    {
        if(r_it->unix_timestamp() > ignore_ts)
        	remote_more_recent = true;

        ++r_it;
    }

    if(local_more_recent && remote_more_recent)
        return CLOCKS_DIVERGE;
    if(local_more_recent)
        return LOCAL_MORE_RECENT;
    if(remote_more_recent)
        return REMOTE_MORE_RECENT;

    return CLOCKS_EQUAL;
}

}
}

