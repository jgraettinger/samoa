
#include "samoa/datamodel/blob.hpp"
#include "samoa/datamodel/clock_util.hpp"
#include "samoa/server/request_state.hpp"
#include "samoa/server/client.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include "samoa/error.hpp"
#include "samoa/log.hpp"
#include <boost/iostreams/device/array.hpp>
#include <boost/iostreams/stream.hpp>

namespace samoa {
namespace datamodel {

namespace bio = boost::iostreams;
namespace spb = samoa::core::protobuf;

void blob::send_blob_value(const server::request_state::ptr_t & rstate,
    const spb::PersistedRecord & record)
{
    spb::SamoaResponse & samoa_response = rstate->get_samoa_response();

    samoa_response.mutable_cluster_clock()->CopyFrom(
        record.cluster_clock());

    for(auto val_it = record.blob_value().begin();
        val_it != record.blob_value().end(); ++val_it)
    {
        rstate->add_response_data_block(val_it->begin(), val_it->end());
    }

    rstate->flush_client_response();
}

merge_result blob::consistent_merge(
    spb::PersistedRecord & local_record,
    const spb::PersistedRecord & remote_record,
    unsigned consistency_horizon)
{
    merge_result result;

    // either record may not actually have a cluster_clock,
    //  in which case it uses the default (empty) instance
    clock_util::clock_ancestry ancestry = clock_util::compare(
        local_record.cluster_clock(), remote_record.cluster_clock(),
        consistency_horizon);

    if(ancestry == clock_util::EQUAL)
    {
        // no change
        result.local_was_updated = false;
        result.remote_is_stale = false;
        return result;
    }
    else if(ancestry == clock_util::MORE_RECENT)
    {
        // local is more recent
        result.local_was_updated = false;
        result.remote_is_stale = true;
        return result;
    }
    else if(ancestry == clock_util::LESS_RECENT)
    {
        // remote is more recent; fully replace local
        local_record.CopyFrom(remote_record);

        result.local_was_updated = true;
        result.remote_is_stale = false;
        return result;
    }

    SAMOA_ASSERT(ancestry == clock_util::DIVERGE);

    // move local_record's clock out
    std::unique_ptr<spb::ClusterClock> local_clock(
        local_record.release_cluster_clock());

    // merge local & remote clocks directly into local_record's clock
    clock_util::compare(
        *local_clock.get(), remote_record.cluster_clock(),
        consistency_horizon,
        local_record.mutable_cluster_clock());

    // merge remote_record's blob_value into local_record
    for(int i = 0; i != remote_record.blob_value_size(); ++i)
    {
        local_record.add_blob_value(remote_record.blob_value(i));
    }

    result.local_was_updated = true;
    result.remote_is_stale = true;
    return result;
}

}
}

