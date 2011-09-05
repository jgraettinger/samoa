
#include "samoa/server/command/set_blob.hpp"
#include "samoa/server/client.hpp"
#include "samoa/server/cluster_state.hpp"
#include "samoa/server/context.hpp"
#include "samoa/server/table_set.hpp"
#include "samoa/server/table.hpp"
#include "samoa/server/peer_set.hpp"
#include "samoa/server/partition.hpp"
#include "samoa/server/local_partition.hpp"
#include "samoa/server/replication_operation.hpp"
#include "samoa/persistence/persister.hpp"
#include "samoa/core/protobuf/fwd.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include "samoa/datamodel/blob.hpp"
#include "samoa/datamodel/clock_util.hpp"
#include <boost/bind.hpp>

namespace samoa {
namespace server {
namespace command {

namespace spb = samoa::core::protobuf;

void set_blob_handler::handle(const client::ptr_t & client)
{
    const spb::SamoaRequest & samoa_request = client->get_request();
    const spb::BlobRequest & blob_request = samoa_request.blob();

    if(!samoa_request.has_blob())
    {
        client->send_error(400, "expected blob field");
        return;
    }

    if(client->get_request_data_blocks().size() != 1)
    {
        client->send_error(400, "expected exactly one data block");
        return;
    }

    if(blob_request.has_cluster_clock() &&
       !datamodel::clock_util::validate(blob_request.cluster_clock()))
    {
        client->send_error(400, "malformed cluster clock");
        return;
    }

    cluster_state::ptr_t cluster_state = \
        client->get_context()->get_cluster_state();

    table::ptr_t table = cluster_state->get_table_set()->get_table_by_name(
        blob_request.table_name());

    if(!table)
    {
        client->send_error(404, "table " + blob_request.table_name());
        return;
    }

    // hash key ring position, & route to responsible partitions
    uint64_t ring_position = table->ring_position(blob_request.key());

    table::ring_route route = table->route_ring_position(ring_position,
        cluster_state->get_peer_set());

    if(!route.primary_partition)
    {
        if(route.secondary_partitions.empty())
        {
            client->send_error(404, "no table partitions");
            return;
        }

        // first secondary partition is lowest-latency peer
        samoa::client::server::ptr_t best_peer =
            route.secondary_partitions.begin()->second;

        if(best_peer)
        {
            cluster_state->get_peer_set()->forward_request(client, best_peer);
        }
        else
        {
            // no connected server instance: defer to peer_set to create one
            cluster_state->get_peer_set()->forward_request(client,
                route.secondary_partitions.begin()->first->get_server_uuid());
        }
        return;
    }

    // else, a primary local partition is available
    spb::PersistedRecord_ptr_t new_record = \
        boost::make_shared<spb::PersistedRecord>();

    // assume the key doesn't exist; initial clock tick for first write
    datamodel::clock_util::tick(*new_record->mutable_cluster_clock(),
        route.primary_partition->get_uuid());

    // assign client's value
    new_record->add_blob_value()->assign(
        boost::asio::buffers_begin(client->get_request_data_blocks()[0]),
        boost::asio::buffers_end(client->get_request_data_blocks()[0]));

    // copy out uuid, 'cause we're about to move route, and c++ doesn't
    //  provide any ordering guarantees of argument evaluation
    core::uuid primary_uuid = route.primary_partition->get_uuid();

    route.primary_partition->get_persister()->put(
        boost::bind(&set_blob_handler::on_put_record,
            shared_from_this(), _1, client, std::move(route), _2),
        boost::bind(&set_blob_handler::on_merge_record,
            shared_from_this(), client, primary_uuid,
            table->get_consistency_horizon(), _1, _2),
        blob_request.key(), new_record);
}

spb::PersistedRecord_ptr_t set_blob_handler::on_merge_record(
    const client::ptr_t & client,
    const core::uuid & partition_uuid,
    unsigned consistency_horizon,
    const spb::PersistedRecord_ptr_t & cur_record,
    const spb::PersistedRecord_ptr_t & new_record)
{
    const spb::SamoaRequest & samoa_request = client->get_request();
    const spb::BlobRequest & blob_request = samoa_request.blob();

    spb::SamoaResponse & samoa_response = client->get_response();
    spb::BlobResponse & blob_response = *samoa_response.mutable_blob();

    // if request included a cluster clock, validate
    //  _exact_ equality against the stored cluster clock
    if(blob_request.has_cluster_clock())
    {
        if(datamodel::clock_util::compare(
                cur_record->cluster_clock(),
                blob_request.cluster_clock()
            ) != datamodel::clock_util::EQUAL)
        {
            // clock doesn't match: abort
            blob_response.set_success(false);
            datamodel::blob::send_blob_value(client, *cur_record);
            return spb::PersistedRecord_ptr_t();
        }
    }

    // clocks match, or the request doesn't have a clock (implicit match)
    //   copy the current clock, and tick to reflect this operation

    spb::ClusterClock & clock = *new_record->mutable_cluster_clock();
    clock.CopyFrom(cur_record->cluster_clock());

    datamodel::clock_util::tick(clock, partition_uuid);
    datamodel::clock_util::prune_record(new_record, consistency_horizon);

    return new_record;
}

void set_blob_handler::on_put_record(
    const boost::system::error_code & ec,
    const client::ptr_t & client,
    table::ring_route & route,
    const spb::PersistedRecord_ptr_t & record)
{
    if(ec)
    {
        client->send_error(500, ec);
        return;
    }

    const spb::SamoaRequest & samoa_request = client->get_request();
    const spb::BlobRequest & blob_request = samoa_request.blob();

    spb::SamoaResponse & samoa_response = client->get_response();
    spb::BlobResponse & blob_response = *samoa_response.mutable_blob();

    cluster_state::ptr_t cluster_state = \
        client->get_context()->get_cluster_state();

    table::ptr_t table = cluster_state->get_table_set()->get_table_by_name(
        blob_request.table_name());

    blob_response.set_success(true);

    if(blob_request.quorum() == 1)
    {
        // quorum met; respond to client immediately
        client->finish_response();

        replication_operation::spawn_replication(
            cluster_state->get_peer_set(), table, std::move(route),
            blob_request.key(), record);
    }
    else
    {
        unsigned quorum = std::min(blob_request.quorum(),
            table->get_replication_factor()) - 1;

        // pass client to replication_operation, which will
        //  respond when the quorum is met
        replication_operation::spawn_replication(
            cluster_state->get_peer_set(), table, std::move(route),
            blob_request.key(), record, client, quorum);
    }
}

}
}
}

