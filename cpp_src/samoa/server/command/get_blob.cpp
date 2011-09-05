
#include "samoa/server/command/get_blob.hpp"
#include "samoa/server/client.hpp"
#include "samoa/server/cluster_state.hpp"
#include "samoa/server/context.hpp"
#include "samoa/server/table_set.hpp"
#include "samoa/server/table.hpp"
#include "samoa/server/peer_set.hpp"
#include "samoa/server/partition.hpp"
#include "samoa/server/local_partition.hpp"
#include "samoa/persistence/persister.hpp"
#include "samoa/core/protobuf/fwd.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include "samoa/datamodel/blob.hpp"
#include <boost/bind.hpp>

namespace samoa {
namespace server {
namespace command {

namespace spb = samoa::core::protobuf;

void get_blob_handler::handle(const client::ptr_t & client)
{
    const spb::SamoaRequest & samoa_request = client->get_request();
    const spb::BlobRequest & blob_request = samoa_request.blob();

    if(!samoa_request.has_blob())
    {
        client->send_error(400, "expected blob field");
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
    route.primary_partition->get_persister()->get(
        boost::bind(&get_blob_handler::on_get_record,
            shared_from_this(), _1, client, _2),
        blob_request.key());
}

void get_blob_handler::on_get_record(
    const boost::system::error_code & ec,
    const client::ptr_t & client,
    const spb::PersistedRecord_ptr_t & record)
{
    if(ec)
    {
        client->send_error(500, ec);
        return;
    }

    spb::SamoaResponse & samoa_response = client->get_response();
    spb::BlobResponse & blob_response = *samoa_response.mutable_blob();

    if(!record)
    {
        blob_response.set_success(false);
        blob_response.mutable_cluster_clock();
        client->finish_response();
        return;
    }

    blob_response.set_success(true);
    datamodel::blob::send_blob_value(client, *record);
}

}
}
}

