
#include "samoa/datamodel/clock_util.hpp"
#include "samoa/core/server_time.hpp"
#include "samoa/core/fwd.hpp"
#include "samoa/error.hpp"

namespace samoa {
namespace datamodel {

namespace spb = samoa::core::protobuf;

// default jitter bound is 1 hour
unsigned clock_util::clock_jitter_bound = 3600;

uint64_t clock_util::generate_author_id()
{
    return core::random::generate_uint64();
}

bool clock_util::validate(const spb::ClusterClock & cluster_clock)
{
    const author_clocks_t & clocks = cluster_clock.author_clock();

    author_clocks_t::const_iterator it = begin(clocks);
    author_clocks_t::const_iterator last_it = it;

    while(it != end(clocks))
    {
        if(it != begin(clocks) && !author_id_comparator()(*last_it, *it))
        {
            return false;
        }
        last_it = it++;
    }
    return true;
}

clock_util::clock_ancestry clock_util::compare(
    const spb::ClusterClock & local_cluster_clock,
    const spb::ClusterClock & remote_cluster_clock,
    unsigned consistency_horizon)
{
    uint64_t ignore_ts = std::max<int64_t>(0,
        core::server_time::get_time() - consistency_horizon);
    uint64_t prune_ts = std::max<int64_t>(0, ignore_ts - clock_jitter_bound);

    bool local_is_legacy = local_cluster_clock.clock_is_pruned() || \
        has_timestamp_reached(local_cluster_clock, ignore_ts);
    bool remote_is_legacy = remote_cluster_clock.clock_is_pruned() || \
        has_timestamp_reached(remote_cluster_clock, prune_ts);

    bool local_more_recent = local_is_legacy && !remote_is_legacy;
    bool remote_more_recent = remote_is_legacy && !local_is_legacy;

    const author_clocks_t & local_authors = \
        local_cluster_clock.author_clock();
    const author_clocks_t & remote_authors = \
        remote_cluster_clock.author_clock();

    author_clocks_t::const_iterator l_it = begin(local_authors);
    author_clocks_t::const_iterator r_it = begin(remote_authors);

    while(l_it != end(local_authors) && r_it != end(remote_authors))
    {
        if(l_it->author_id() < r_it->author_id())
        {
            if(l_it->unix_timestamp() > ignore_ts ||
                !remote_cluster_clock.clock_is_pruned())
            {
                local_more_recent = true;
            }
            ++l_it;
        }
        else if(l_it->author_id() > r_it->author_id())
        {
            if(r_it->unix_timestamp() > prune_ts ||
                !local_cluster_clock.clock_is_pruned())
            {
                remote_more_recent = true;
            }
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
    while(l_it != end(local_authors))
    {
        if(l_it->unix_timestamp() > ignore_ts ||
            !remote_cluster_clock.clock_is_pruned())
        {
            local_more_recent = true;
        }
        ++l_it;
    }
    while(r_it != end(remote_authors))
    {
        if(r_it->unix_timestamp() > prune_ts ||
            !local_cluster_clock.clock_is_pruned())
        {
            remote_more_recent = true;
        }
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

