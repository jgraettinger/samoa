
#include "samoa/datamodel/counter.hpp"
#include "samoa/datamodel/clock_util.hpp"
#include "samoa/request/request_state.hpp"
#include "samoa/core/server_time.hpp"
#include "samoa/error.hpp"
#include "samoa/log.hpp"

namespace samoa {
namespace datamodel {

namespace spb = samoa::core::protobuf;

void counter::update(spb::PersistedRecord & record,
    const core::uuid & partition_uuid,
    int64_t increment)
{
    auto update = [&](bool insert_before, unsigned index) -> void
    {
    	if(insert_before)
        {
            core::protobuf::insert_before(
                *record.mutable_counter_value(), index) += increment;
        }
        else
        {
        	record.set_counter_value(index,
        	    record.counter_value(index) + increment);
        }
    };
    clock_util::tick(*record.mutable_cluster_clock(),
        partition_uuid, update);
}

bool counter::prune(spb::PersistedRecord & record,
    unsigned consistency_horizon)
{
    auto update = [&record](unsigned index) -> bool
    {
    	if(record.counter_value(index))
        {
        	// fold into consistent count value
        	record.set_counter_consistent_value(
        	    record.counter_consistent_value() + \
        	    record.counter_value(index));
        }
        core::protobuf::remove_before(
            *record.mutable_counter_value(), index + 1);
        return true;
    };
    clock_util::prune(record, consistency_horizon, update);

    if(record.has_expire_timestamp() && \
       record.expire_timestamp() < core::server_time::get_time())
    {
        return true;
    }
    return false;
}

merge_result counter::merge(
    spb::PersistedRecord & local_record,
    const spb::PersistedRecord & remote_record,
    unsigned consistency_horizon)
{
    typedef google::protobuf::RepeatedField<int64_t> counter_values_t;

    counter_values_t & lhs_values = *local_record.mutable_counter_value();
    const counter_values_t & rhs_values = remote_record.counter_value();

    counter_values_t::iterator lhs_it = begin(lhs_values);
    counter_values_t::const_iterator rhs_it = begin(rhs_values);

    auto update = [&](clock_util::merge_step state) -> void
    {
    	if(state == clock_util::LHS_RHS_EQUAL)
        {
            SAMOA_ASSERT(*lhs_it == *rhs_it);
            ++lhs_it; ++rhs_it;
        }
        else if(state == clock_util::LHS_ONLY)
        {
        	++lhs_it;
        }
        else if(state == clock_util::RHS_ONLY)
        {
            spb::insert_before(lhs_values, lhs_it) = *rhs_it;
            ++rhs_it;
        }
        else if(state == clock_util::LHS_NEWER)
        {
        	++lhs_it; ++rhs_it;
        }
        else if(state == clock_util::RHS_NEWER)
        {
        	*lhs_it = *rhs_it;
        	++lhs_it; ++rhs_it;
        }
        else if(state == clock_util::RHS_SKIP)
        {
            ++rhs_it;
        }
    };
    return clock_util::merge(
        *local_record.mutable_cluster_clock(),
        remote_record.cluster_clock(),
        consistency_horizon,
        update);
}

int64_t counter::value(const spb::PersistedRecord & record)
{
	int64_t aggregate = record.counter_consistent_value();

    for(int64_t value : record.counter_value())
    {
        aggregate += value; 
    }
    return aggregate;
}

}
}

