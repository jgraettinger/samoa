
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

    // hash key ring position
    uint64_t ring_position = table->ring_position(blob_request.key());

    // route the position to responsible partitions & primary partition
    partition::ptr_t primary_partition;
    table::ring_t all_partitions;

    bool primary_is_local = table->route_ring_position(ring_position,
        cluster_state->get_peer_set(), primary_partition, all_partitions);

    if(all_partitions.empty())
    {
        client->send_error(404, "no table partitions");
        return;
    }

    if(!primary_partition)
    {
        client->send_error(503, "no available peer for forwarding");
        return;
    }

    // if the primary partition isn't local, forward to it's peer server
    if(!primary_is_local)
    {
        cluster_state->get_peer_set()->forward_request(
            client, primary_partition->get_server_uuid());
        return;
    }

    local_partition & local_primary = \
        dynamic_cast<local_partition &>(*primary_partition);

    local_primary.get_persister()->get(boost::bind(
        &get_blob_handler::on_get_record, shared_from_this(),
        _1, client, _2), blob_request.key());
}

void get_blob_handler::on_get_record(
    const boost::system::error_code & ec,
    const client::ptr_t & client,
    const persistence::record * record)
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

    spb::Value value;
    value.ParseFromArray(record->value_begin(), record->value_length());

    datamodel::blob::send_blob_value(client, value);
}

}
}
}

