
#include "samoa/datamodel/blob.hpp"
#include "samoa/datamodel/clock_util.hpp"
#include "samoa/request/request_state.hpp"
#include "samoa/core/server_time.hpp"
#include "samoa/error.hpp"
#include "samoa/log.hpp"

namespace samoa {
namespace datamodel {

namespace spb = samoa::core::protobuf;
using std::begin;
using std::end;

template<typename AssignValue>
void update_common(spb::PersistedRecord & record, uint64_t author_id,
    const AssignValue & assign_value)
{
    // any previous divergence has been repaired
    record.clear_blob_consistent_value();
    for(std::string & value : *record.mutable_blob_value())
    {
        value.clear();
    }

    auto update = [&](bool insert_before, unsigned index) -> void
    {
        if(insert_before)
        {
            assign_value(core::protobuf::insert_before(
                *record.mutable_blob_value(), index));
        }
        else
        {
            assign_value(*record.mutable_blob_value(index));
        }
    };
    clock_util::tick(*record.mutable_cluster_clock(), author_id, update);
}

void blob::update(spb::PersistedRecord & record, uint64_t author_id,
    const core::buffer_regions_t & new_value)
{
    auto assign_value = [&](std::string & out)
    {
        core::copy_regions_into(new_value, out);
    };
    update_common(record, author_id, assign_value);
}

void blob::update(spb::PersistedRecord & record, uint64_t author_id,
    const std::string & value)
{
    auto assign_value = [&](std::string & out)
    {
        out = value;
    };
    update_common(record, author_id, assign_value);
}

bool blob::prune(spb::PersistedRecord & record,
    unsigned consistency_horizon)
{
    if(record.has_expire_timestamp() && \
       record.expire_timestamp() < core::server_time::get_time())
    {
        return true;
    }

    auto update = [&](unsigned index) -> void
    {
        if(record.blob_value(index).size())
        {
            // fold into consistent blob values
            record.add_blob_consistent_value(record.blob_value(index));
        }
        core::protobuf::remove_before(
            *record.mutable_blob_value(), index + 1);
    };
    clock_util::prune(record, consistency_horizon, update);

    if(!record.blob_value_size() && !record.consistent_blob_value_size())
    {
        // clock horizon has passed on a deleted blob
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

    bool is_legacy_merge = false;

    blob_values_t & local_values = *local_record.mutable_blob_value();
    const blob_values_t & remote_values = remote_record.blob_value();

    blob_values_t::iterator l_it = begin(local_values);
    blob_values_t::const_iterator r_it = begin(remote_values);

    auto update = [&](clock_util::merge_step state,
        bool local_is_legacy, bool remote_is_legacy) -> void
    {
        if(!is_legacy_merge && !local_is_legacy && remote_is_legacy)
        {
            is_legacy_merge = true;
            local_record.blob_consistent_value().CopyFrom(
                remote_record.blob_consistent_value());
        }

        if(state == clock_util::LAUTH_RAUTH_EQUAL)
        {
            if(!r_it->size())
            {
                // a remote repair/replace has occurred
                l_it->clear();
                local_record.clear_blob_consistent_value();
            }
            ++l_it; ++r_it;
        }
        else if(state == clock_util::RAUTH_PRUNED)
        {
            if(remote_is_legacy && !remote_record.blob_consistent_value_size())
            {
                // a remote repair/replace has occurred
                l_it->clear();
                local_record.clear_blob_consistent_value();
            }
            ++l_it;
        }
        else if(state == clock_util::LAUTH_PRUNED)
        {
            if(is_legacy_merge && r_it->size())
            {
                // as this is a legacy merge, we didn't know about
                //  this value & haven't pruned it; fold it in now
                local_record.add_blob_consistent_value(*r_it);
            }
            ++r_it;
        }
        else if(state == clock_util::LAUTH_ONLY)
        {
            ++l_it;
        }
        else if(state == clock_util::RAUTH_ONLY)
        {
            spb::insert_before(local_values, l_it) = *r_it;
            ++r_it;
        }
        else if(state == clock_util::LAUTH_NEWER)
        {
            ++l_it; ++r_it;
        }
        else if(state == clock_util::RAUTH_NEWER)
        {
            *l_it = *r_it;
            ++l_it; ++r_it;
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

