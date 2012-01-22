
#include "samoa/core/protobuf/sequence_util.hpp"
#include "samoa/core/server_time.hpp"
#include "samoa/log.hpp"

namespace samoa {
namespace datamodel {

namespace spb = samoa::core::protobuf;

using std::begin;
using std::end;

struct author_id_comparator
{
    bool operator()(const spb::AuthorClock & lhs, const spb::AuthorClock & rhs)
    { return lhs.author_id() < rhs.author_id(); }

    bool operator()(const spb::AuthorClock & lhs, uint64_t rhs)
    { return lhs.author_id() < rhs; }

    bool operator()(uint64_t lhs, const spb::AuthorClock & rhs)
    { return lhs < rhs.author_id(); }
};

template<typename DatamodelUpdate>
void clock_util::tick(spb::ClusterClock & cluster_clock,
    uint64_t author_id, const DatamodelUpdate & datamodel_update)
{
    author_clocks_t & clocks = *cluster_clock.mutable_author_clock();
    author_clocks_t::iterator it = std::lower_bound(
        clocks.begin(), clocks.end(), author_id, author_id_comparator());

    size_t ind = std::distance(begin(clocks), it);

    if(it == end(clocks) || it->author_id() != author_id)
    {
        // no current clock for this partition; add one
        spb::AuthorClock & new_clock = insert_before(clocks, it);
        new_clock.set_author_id(author_id);
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
    unsigned consistency_horizon, const DatamodelUpdate & datamodel_update)
{
    uint64_t ignore_ts = std::max<int64_t>(0,
        core::server_time::get_time() - consistency_horizon);
    uint64_t prune_ts = std::max<int64_t>(0, ignore_ts - clock_jitter_bound);

    spb::ClusterClock & cluster_clock = *record.mutable_cluster_clock();
    author_clocks_t & clocks = *cluster_clock.mutable_author_clock();

    author_clocks_t::iterator it = clocks.begin();
    author_clocks_t::iterator last_it = it;

    while(it != end(clocks))
    {
        SAMOA_ASSERT(last_it == it || author_id_comparator()(*last_it, *it));

        if(it->unix_timestamp() <= prune_ts)
        {
            datamodel_update(std::distance(begin(clocks), it));

            core::protobuf::remove_before(clocks, ++it);
            last_it = (it == begin(clocks)) ? it : it - 1;

            if(!cluster_clock.clock_is_pruned())
            {
            	// effectively sets to true
                cluster_clock.clear_clock_is_pruned();
            }
        }
        else
            last_it = it++;
    }
    if(clocks.size() == 0 && cluster_clock.clock_is_pruned())
    {
    	// no need to serialize the default cluster clock
        record.clear_cluster_clock();
    }
}

inline bool clock_util::has_timestamp_reached(const spb::ClusterClock & clock,
    uint64_t timestamp)
{
    for(const spb::AuthorClock & author_clock : clock.author_clock())
    {
        if(author_clock.unix_timestamp() <= timestamp)
        	return true;
    }
    return false;
}

template<typename DatamodelUpdate>
merge_result clock_util::merge(
    spb::ClusterClock & local_cluster_clock,
    const spb::ClusterClock & remote_cluster_clock,
    unsigned consistency_horizon,
    const DatamodelUpdate & datamodel_update)
{
    uint64_t ignore_ts = std::max<int64_t>(0,
        core::server_time::get_time() - consistency_horizon);
    uint64_t prune_ts = std::max<int64_t>(0, ignore_ts - clock_jitter_bound);

    bool local_is_legacy = local_cluster_clock.clock_is_pruned() || \
        has_timestamp_reached(local_cluster_clock, ignore_ts);
    bool remote_is_legacy = remote_cluster_clock.clock_is_pruned() || \
        has_timestamp_reached(remote_cluster_clock, ignore_ts);

    merge_result result;
    result.local_was_updated = remote_is_legacy && !local_is_legacy;
    result.remote_is_stale = local_is_legacy && !remote_is_legacy;

    if(!local_is_legacy && remote_cluster_clock.clock_is_pruned())
    {
        // inherit remote clock's pruning status of 'true' (default)
        //  datamodel is responsible for inheriting non-versioned content
        local_cluster_clock.clear_clock_is_pruned();
    }

    author_clocks_t & local_authors = \
        *local_cluster_clock.mutable_author_clock();
    const author_clocks_t & remote_authors = \
        remote_cluster_clock.author_clock();

    author_clocks_t::iterator l_it = begin(local_authors);
    author_clocks_t::const_iterator r_it = begin(remote_authors);

    while(l_it != end(local_authors) && r_it != end(remote_authors))
    {
        if(l_it->author_id() < r_it->author_id())
        {
            if(l_it->unix_timestamp() > ignore_ts ||
                !remote_cluster_clock.clock_is_pruned())
            {
                result.remote_is_stale = true;
                datamodel_update(LAUTH_ONLY,
                    local_is_legacy, remote_is_legacy);
            }
            else
            	datamodel_update(RAUTH_PRUNED,
                    local_is_legacy, remote_is_legacy);

            ++l_it;
        }
        else if(l_it->author_id() > r_it->author_id())
        {
            if(r_it->unix_timestamp() > prune_ts ||
                !local_cluster_clock.clock_is_pruned())
            {
                result.local_was_updated = true;
                datamodel_update(RAUTH_ONLY,
                    local_is_legacy, remote_is_legacy);

                core::protobuf::insert_before(local_authors, l_it) = *r_it;
            }
            else
                datamodel_update(LAUTH_PRUNED,
                    local_is_legacy, remote_is_legacy);

            ++r_it;
        }
        else
        {
            if(l_it->unix_timestamp() > r_it->unix_timestamp())
            {
                result.remote_is_stale = true;
                datamodel_update(LAUTH_NEWER,
                    local_is_legacy, remote_is_legacy);
            }
            else if(l_it->unix_timestamp() < r_it->unix_timestamp())
            {
                result.local_was_updated = true;
                datamodel_update(RAUTH_NEWER,
                    local_is_legacy, remote_is_legacy);

                l_it->set_unix_timestamp(r_it->unix_timestamp());

                if(r_it->has_lamport_tick())
                    l_it->set_lamport_tick(r_it->lamport_tick());
                else
                	l_it->clear_lamport_tick();
            }
            else
            {
                if(l_it->lamport_tick() > r_it->lamport_tick())
                {
                    result.remote_is_stale = true;
                    datamodel_update(LAUTH_NEWER,
                        local_is_legacy, remote_is_legacy);
                }
                else if(l_it->lamport_tick() < r_it->lamport_tick())
                {
                    result.local_was_updated = true;
                    datamodel_update(RAUTH_NEWER,
                        local_is_legacy, remote_is_legacy);

                    l_it->set_lamport_tick(r_it->lamport_tick());
                }
                else
                {
                    datamodel_update(LAUTH_RAUTH_EQUAL,
                        local_is_legacy, remote_is_legacy);
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
            result.remote_is_stale = true;
            datamodel_update(LAUTH_ONLY,
                local_is_legacy, remote_is_legacy);
        }
        else
        	datamodel_update(RAUTH_PRUNED,
                local_is_legacy, remote_is_legacy);

        ++l_it;
    }
    while(r_it != end(remote_authors))
    {
        if(r_it->unix_timestamp() > prune_ts ||
            !local_cluster_clock.clock_is_pruned())
        {
            result.local_was_updated = true;
            datamodel_update(RAUTH_ONLY,
                local_is_legacy, remote_is_legacy);

            core::protobuf::insert_before(local_authors, l_it) = *r_it;
        }
        else
            datamodel_update(LAUTH_PRUNED,
                local_is_legacy, remote_is_legacy);

        ++r_it;
    }

    return result;
}

}
}

