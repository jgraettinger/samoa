
#include "samoa/datamodel/counter.hpp"
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

void counter::update(spb::PersistedRecord & record, uint64_t author_id,
    int64_t increment)
{
    auto update = [&](bool insert_before, unsigned index) -> void
    {
        if(insert_before)
        {
            core::protobuf::insert_before(
                *record.mutable_counter_value(), index) = increment;
        }
        else
        {
            record.set_counter_value(index,
                record.counter_value(index) + increment);
        }
    };
    clock_util::tick(*record.mutable_cluster_clock(), author_id, update);
}

bool counter::prune(spb::PersistedRecord & record,
    unsigned consistency_horizon)
{
    if(record.has_expire_timestamp() && \
       record.expire_timestamp() < core::server_time::get_time())
    {
        return true;
    }

    auto update = [&record](unsigned index)
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
    };
    clock_util::prune(record, consistency_horizon, update);

    return false;
}

merge_result counter::merge(
    spb::PersistedRecord & local_record,
    const spb::PersistedRecord & remote_record,
    unsigned consistency_horizon)
{
    typedef google::protobuf::RepeatedField<int64_t> counter_values_t;

    bool is_legacy_merge = false;

    counter_values_t & local_values = *local_record.mutable_counter_value();
    const counter_values_t & remote_values = remote_record.counter_value();

    counter_values_t::iterator l_it = begin(local_values);
    counter_values_t::const_iterator r_it = begin(remote_values);

    int64_t debug_delta = remote_record.counter_consistent_value() \
        - local_record.counter_consistent_value();

    auto update = [&](clock_util::merge_step state,
        bool local_is_legacy, bool remote_is_legacy) -> void
    {
        if(!is_legacy_merge && !local_is_legacy && remote_is_legacy)
        {
            is_legacy_merge = true;
            local_record.set_counter_consistent_value(
                remote_record.counter_consistent_value());

            debug_delta = 0;
        }

        if(state == clock_util::LAUTH_RAUTH_EQUAL)
        {
            SAMOA_ASSERT(*l_it == *r_it);
            ++l_it; ++r_it;
        }
        else if(state == clock_util::RAUTH_PRUNED)
        {
            SAMOA_ASSERT(remote_is_legacy);
            debug_delta -= *l_it;

            ++l_it;
        }
        else if(state == clock_util::LAUTH_PRUNED)
        {
            SAMOA_ASSERT(!is_legacy_merge);
            debug_delta += *r_it;

            ++r_it;
        }
        else if(state == clock_util::LAUTH_ONLY)
        {
            debug_delta -= *l_it;
            ++l_it;
        }
        else if(state == clock_util::RAUTH_ONLY)
        {
            spb::insert_before(local_values, l_it) = *r_it;
            ++r_it;
        }
        else if(state == clock_util::LAUTH_NEWER)
        {
            debug_delta -= *l_it - *r_it;
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

    SAMOA_ASSERT(value(local_record) + debug_delta == value(remote_record));
    return result;
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

