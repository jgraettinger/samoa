
#include "samoa/server/command/set_blob.hpp"
#include "samoa/server/client.hpp"
#include "samoa/server/table.hpp"
#include "samoa/server/peer_set.hpp"
#include "samoa/server/partition.hpp"
#include "samoa/server/local_partition.hpp"
#include "samoa/server/replication.hpp"
#include "samoa/persistence/persister.hpp"
#include "samoa/request/request_state.hpp"
#include "samoa/request/state_exception.hpp"
#include "samoa/core/protobuf/fwd.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include "samoa/datamodel/blob.hpp"
#include "samoa/datamodel/clock_util.hpp"
#include "samoa/log.hpp"
#include <boost/bind.hpp>

namespace samoa {
namespace server {
namespace command {

namespace spb = samoa::core::protobuf;
using std::begin;
using std::end;

void set_blob_handler::handle(const request::state::ptr_t & rstate)
{
    if(rstate->get_request_data_blocks().empty())
    {
        throw request::state_exception(400,
            "expected exactly one data block");
    }

    rstate->load_table_state();
    rstate->load_route_state();

    if(!rstate->get_primary_partition())
    {
        // no primary partition; forward to a better peer
        rstate->get_peer_set()->forward_request(rstate);
        return;
    }

    rstate->load_replication_state();

    spb::PersistedRecord & record = rstate->get_remote_record();

    // assume the key doesn't exist, and we're creating a new record
    datamodel::blob::update(record,
        rstate->get_primary_partition_uuid(),
        rstate->get_request_data_blocks()[0]);

    rstate->get_primary_partition()->get_persister()->put(
        boost::bind(&set_blob_handler::on_put,
            shared_from_this(), _1, _2, rstate),
        boost::bind(&set_blob_handler::on_merge,
            shared_from_this(), _1, _2, rstate),
        rstate->get_key(),
        rstate->get_remote_record(),
        rstate->get_local_record());
}

datamodel::merge_result set_blob_handler::on_merge(
    spb::PersistedRecord & local_record,
    const spb::PersistedRecord &,
    const request::state::ptr_t & rstate)
{
    datamodel::merge_result result;
    result.local_was_updated = false;
    result.remote_is_stale = false;

    // if request included a cluster clock, validate
    //  equality against the stored cluster clock
    if(rstate->get_samoa_request().has_cluster_clock())
    {
        if(datamodel::clock_util::compare(
                local_record.cluster_clock(),
                rstate->get_samoa_request().cluster_clock(),
                rstate->get_table()->get_consistency_horizon()
            ) != datamodel::clock_util::CLOCKS_EQUAL)
        {
            // clock doesn't match: abort
            return result;
        }
    }

    // clocks match, or the request doesn't have a clock (implicit match)
    //   update the record to 'repair' any divergence with the new value

    datamodel::blob::update(local_record,
        rstate->get_primary_partition_uuid(),
        rstate->get_request_data_blocks()[0]);

    datamodel::blob::prune(local_record,
        rstate->get_table()->get_consistency_horizon());

    result.local_was_updated = true;
    result.remote_is_stale = true;
    return result;
}

void set_blob_handler::on_put(
    const boost::system::error_code & ec,
    const datamodel::merge_result & merge_result,
    const request::state::ptr_t & rstate)
{
    if(ec)
    {
        rstate->send_error(500, ec);
        return;
    }

    if(!merge_result.local_was_updated)
    {
        spb::SamoaResponse & response = rstate->get_samoa_response();

        response.set_success(false);
        response.mutable_cluster_clock()->CopyFrom(
            rstate->get_local_record().cluster_clock());

        datamodel::blob::value(rstate->get_local_record(),
            [&](const std::string & value)
            { rstate->add_response_data_block(begin(value), end(value)); });

        rstate->flush_response();
        return;
    }

    // local write was a success
    rstate->get_samoa_response().set_success(true);

    // count the local write, and respond to client if quorum is met
    if(rstate->peer_replication_success())
    {
        on_replicated_write(rstate);
    }

    // replicate the update to peers
    replication::replicated_write(
        boost::bind(&set_blob_handler::on_replicated_write,
            shared_from_this(), rstate),
        rstate);
}

void set_blob_handler::on_replicated_write(
    const request::state::ptr_t & rstate)
{
    rstate->get_samoa_response().set_replication_success(
        rstate->get_peer_success_count());
    rstate->get_samoa_response().set_replication_failure(
        rstate->get_peer_failure_count());

    rstate->flush_response();
}

}
}
}

