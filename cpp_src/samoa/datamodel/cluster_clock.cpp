
#include "samoa/datamodel/cluster_clock.hpp"
#include "samoa/datamodel/packed_unsigned.hpp"
#include "samoa/core/server_time.hpp"

namespace samoa {
namespace datamodel {

void cluster_clock::tick(const core::uuid & partition_uuid)
{
    iterator it = std::lower_bound(begin(), end(), partition_uuid,
        partition_clock::uuid_comparator());

    // no current clock for this partition? add one
    if(it == end() || it->partition_uuid != partition_uuid)
    {
        it = insert(it, partition_clock(partition_uuid));
    }

    it->unix_timestamp = core::server_time::get_time();
    it->lamport_tick += 1;
}

std::ostream & operator << (std::ostream & s, const cluster_clock & r)
{
    s << packed_unsigned(r.size());
    for(cluster_clock::const_iterator it = r.begin(); it != r.end(); ++it)
    {
        s.write((const char *)it->partition_uuid.data, sizeof(it->partition_uuid.data));
        s << packed_unsigned(it->unix_timestamp);
        s << packed_unsigned(it->lamport_tick);
    }
    return s;
}

std::istream & operator >> (std::istream & s, cluster_clock & r)
{
    packed_unsigned clock_size;
    s >> clock_size;

    r.resize(clock_size);
    for(cluster_clock::iterator it = r.begin(); it != r.end(); ++it)
    {
        packed_unsigned tmp;

        s.read((char*)it->partition_uuid.data, sizeof(it->partition_uuid.data));
        s >> tmp; it->unix_timestamp = tmp.value;
        s >> tmp; it->lamport_tick = tmp.value;
    }
    return s;
}

cluster_clock::clock_ancestry cluster_clock::compare(
    const cluster_clock & lhs, const cluster_clock & rhs,
    cluster_clock * merged_clock_out)
{
    if(merged_clock_out)
        merged_clock_out->clear();

    bool lhs_more_recent = false;
    bool rhs_more_recent = false;

    cluster_clock::const_iterator l_it = lhs.begin();
    cluster_clock::const_iterator r_it = rhs.begin();

    while(l_it != lhs.end() && r_it != rhs.end())
    {
        if(l_it->partition_uuid < r_it->partition_uuid)
        {
            lhs_more_recent = true;

            if(merged_clock_out)
                merged_clock_out->push_back(*l_it);

            ++l_it;
        }
        else if(l_it->partition_uuid > r_it->partition_uuid)
        {
            rhs_more_recent = true;

            if(merged_clock_out)
                merged_clock_out->push_back(*r_it);

            ++r_it;
        }
        else
        {
            // partition_uuid's are equal - check timestamps

            if(l_it->unix_timestamp < r_it->unix_timestamp)
            {
                rhs_more_recent = true;

                if(merged_clock_out)
                    merged_clock_out->push_back(*r_it);
            }
            else if(l_it->unix_timestamp > r_it->unix_timestamp)
            {
                lhs_more_recent = true;

                if(merged_clock_out)
                    merged_clock_out->push_back(*l_it);
            }

            // timestamps are the same

            else if(l_it->lamport_tick < r_it->lamport_tick)
            {
                rhs_more_recent = true;

                if(merged_clock_out)
                    merged_clock_out->push_back(*r_it);
            }
            else if(l_it->lamport_tick > r_it->lamport_tick)
            {
                lhs_more_recent = true;

                if(merged_clock_out)
                    merged_clock_out->push_back(*l_it);
            }

            // partition clocks are identical

            else if(merged_clock_out)
            {
                merged_clock_out->push_back(*l_it);
            }

            ++l_it; ++r_it;
        }
    }
    while(l_it != lhs.end())
    {
        lhs_more_recent = true;

        if(merged_clock_out)
            merged_clock_out->push_back(*l_it);

        ++l_it;            
    }
    while(r_it != rhs.end())
    {
        rhs_more_recent = true;

        if(merged_clock_out)
            merged_clock_out->push_back(*r_it);

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

