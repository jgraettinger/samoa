
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

    auto update = [&](unsigned index)
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

    if(!record.blob_value_size() && !record.blob_consistent_value_size())
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

    // we remove consistent values only if remote is legacy, has no
    //  consistent values, and has no prunable non-empty values
    bool prune_consistent_values = \
        (remote_record.blob_consistent_value_size() == 0);

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
            prune_consistent_values = false;

            local_record.mutable_blob_consistent_value()->CopyFrom(
                remote_record.blob_consistent_value());
        }
        if(!remote_is_legacy)
            prune_consistent_values = false;

        if(state == clock_util::LAUTH_RAUTH_EQUAL)
        {
            if(!r_it->size())
            {
                // a remote repair of this specific value has occurred
                l_it->clear();
            }
            ++l_it; ++r_it;
        }
        else if(state == clock_util::RAUTH_PRUNED)
        {
            SAMOA_ASSERT(remote_is_legacy);

            if(!remote_record.blob_consistent_value_size())
            {
                // a remote repair of this specific value has occurred
                l_it->clear();
            }
            ++l_it;
        }
        else if(state == clock_util::LAUTH_PRUNED)
        {
            SAMOA_ASSERT(!is_legacy_merge);

            if(r_it->size())
            {
                // implication is remote hasn't repaired consistent values
                prune_consistent_values = false;
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

    merge_result result = clock_util::merge(
        *local_record.mutable_cluster_clock(),
        remote_record.cluster_clock(),
        consistency_horizon,
        update);

    if(prune_consistent_values)
    {
        local_record.clear_blob_consistent_value();
    }
    return result;
}

}
}

