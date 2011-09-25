
#include "samoa/server/command/set_blob.hpp"
#include "samoa/server/client.hpp"
#include "samoa/server/table.hpp"
#include "samoa/server/peer_set.hpp"
#include "samoa/server/partition.hpp"
#include "samoa/server/local_partition.hpp"
#include "samoa/server/replication.hpp"
#include "samoa/server/request_state.hpp"
#include "samoa/persistence/persister.hpp"
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

void set_blob_handler::handle(const request_state::ptr_t & rstate)
{
    if(!rstate->get_table())
    {
        rstate->send_client_error(400, "expected table name or UUID");
        return;
    }
    if(rstate->get_key().empty())
    {
        rstate->send_client_error(400, "expected key");
        return;
    }
    if(!rstate->get_primary_partition())
    {
        rstate->get_peer_set()->forward_request(rstate);
        return;
    }
    if(rstate->get_request_data_blocks().size() != 1)
    {
        rstate->send_client_error(400, "expected exactly one data block");
        return;
    }

    spb::PersistedRecord & record = rstate->get_remote_record();

    // assume the key doesn't exist; initial clock tick for first write
    datamodel::clock_util::tick(*record.mutable_cluster_clock(),
        rstate->get_primary_partition()->get_uuid());

    // assign client's value
    record.add_blob_value()->assign(
        boost::asio::buffers_begin(rstate->get_request_data_blocks()[0]),
        boost::asio::buffers_end(rstate->get_request_data_blocks()[0]));

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
    const spb::PersistedRecord & remote_record,
    const request_state::ptr_t & rstate)
{
    datamodel::merge_result result;
    result.local_was_updated = false;
    result.remote_is_stale = false;

    // if request included a cluster clock, validate
    //  _exact_ equality against the stored cluster clock
    if(rstate->get_samoa_request().has_cluster_clock())
    {
        if(datamodel::clock_util::compare(
                local_record.cluster_clock(),
                rstate->get_samoa_request().cluster_clock()
            ) != datamodel::clock_util::EQUAL)
        {
            // clock doesn't match: abort
            return result;
        }
    }

    // clocks match, or the request doesn't have a clock (implicit match)
    //   tick the local clock to reflect this operation

    datamodel::clock_util::tick(*local_record.mutable_cluster_clock(),
        rstate->get_primary_partition()->get_uuid());

    datamodel::clock_util::prune_record(local_record,
        rstate->get_table()->get_consistency_horizon());

    local_record.mutable_blob_value()->CopyFrom(remote_record.blob_value());

    result.local_was_updated = true;
    result.remote_is_stale = true;
    return result;
}

void set_blob_handler::on_put(
    const boost::system::error_code & ec,
    const datamodel::merge_result & merge_result,
    const request_state::ptr_t & rstate)
{
    if(ec)
    {
        rstate->send_client_error(500, ec);
        return;
    }

    if(!merge_result.local_was_updated)
    {
        rstate->get_samoa_response().set_success(false);
        datamodel::blob::send_blob_value(rstate,
            rstate->get_local_record());
        return;
    }

    rstate->get_samoa_response().set_success(true);

    if(!rstate->get_client_quorum())
    {
        rstate->flush_client_response();
    }

    replication::replicated_write(
        boost::bind(&set_blob_handler::on_replicated_write,
            shared_from_this(), rstate),
        rstate);
}

void set_blob_handler::on_replicated_write(const request_state::ptr_t & rstate)
{
    if(!rstate->get_client())
    {
        return;
    }

    if(rstate->get_samoa_request().has_requested_quorum())
    {
        rstate->get_samoa_response().set_replication_success(
            rstate->get_peer_success_count() + 1 /* for local write */);
        rstate->get_samoa_response().set_replication_failure(
            rstate->get_peer_error_count());
    }

    rstate->flush_client_response();
}

}
}
}

