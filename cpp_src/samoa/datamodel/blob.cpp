
#include "samoa/datamodel/blob.hpp"
#include "samoa/datamodel/clock_util.hpp"
#include "samoa/request/request_state.hpp"
#include "samoa/core/server_time.hpp"
#include "samoa/error.hpp"
#include "samoa/log.hpp"

namespace samoa {
namespace datamodel {

namespace spb = samoa::core::protobuf;

void blob::update(spb::PersistedRecord & record,
    const core::uuid & partition_uuid,
    const core::buffer_regions_t & new_value)
{
	// any previous divergence has been repaired
    record.clear_consistent_blob_value();
    for(std::string & value : *record.mutable_blob_value())
    {
    	value.clear();
    }

    auto update = [&](bool insert_before, unsigned index) -> void
    {
        if(insert_before)
        {
        	core::copy_regions_into(new_value,
        	    core::protobuf::insert_before(
        	        *record.mutable_blob_value(), index));
        }
        else
        {
        	core::copy_regions_into(new_value,
        	    *record.mutable_blob_value(index));
        }
    };
    clock_util::tick(*record.mutable_cluster_clock(),
        partition_uuid, update);
}

void blob::update(spb::PersistedRecord & record,
    const core::uuid & partition_uuid,
    const std::string & value)
{
    auto update = [&](bool insert_before, unsigned index) -> void
    {
        if(insert_before)
        {
        	core::protobuf::insert_before(
        	    *record.mutable_blob_value(), index) = value;
        }
        else
        {
            record.set_blob_value(index, value);
        }
    };
    clock_util::tick(*record.mutable_cluster_clock(),
        partition_uuid, update);
}

bool blob::prune(spb::PersistedRecord & record,
    unsigned consistency_horizon)
{
    auto update = [&](unsigned index) -> void
    {
        if(record.blob_value(index).size())
        {
        	// fold into consistent blob values
            record.add_consistent_blob_value(record.blob_value(index));
        }
        core::protobuf::remove_before(
            *record.mutable_blob_value(), index + 1);
    };
    clock_util::prune(record, consistency_horizon, update);

    if(record.has_expire_timestamp() && \
       record.expire_timestamp() < core::server_time::get_time())
    {
        return true;
    }
    if(!record.blob_value_size() && !record.consistent_blob_value_size())
    {
    	return true;
    }
    return false;
}

merge_result blob::merge(
    spb::PersistedRecord & local_record,
    const spb::PersistedRecord & remote_record,
    unsigned consistency_horizon)
{
    typedef google::protobuf::RepeatedPtrField<std::string> blob_values_t;

    blob_values_t & lhs_values = *local_record.mutable_blob_value();
    const blob_values_t & rhs_values = remote_record.blob_value();

    blob_values_t::iterator lhs_it = begin(lhs_values);
    blob_values_t::const_iterator rhs_it = begin(rhs_values);

    auto update = [&](clock_util::merge_step state) -> void
    {
    	if(state == clock_util::LHS_RHS_EQUAL)
        {
        	if(!rhs_it->size())
            {
            	// if rhs is clear, implication is that remote
            	//  has already read-repaired this value;
            	//  we can thus discard it
        		lhs_it->clear();
            }

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

}
}

