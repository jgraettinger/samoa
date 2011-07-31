
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
#include "samoa/core/protobuf/samoa.pb.h"
#include "samoa/datamodel/blob.hpp"
#include <boost/bind.hpp>

namespace samoa {
namespace server {
namespace command {

namespace spb = samoa::core::protobuf;

void get_blob_handler::handle(const client::ptr_t & client)
{
    const spb::SamoaRequest & samoa_req = client->get_request();
    const spb::GetBlobRequest & get_blob = samoa_req.get_blob();

    if(!samoa_req.has_get_blob())
    {
        client->send_error(400, "expected get_blob");
        return;
    }

    cluster_state::ptr_t cluster_state = \
        client->get_context()->get_cluster_state();

    table::ptr_t table = cluster_state->get_table_set()->get_table_by_name(
        get_blob.table_name());

    if(!table)
    {
        client->send_error(404, "table " + get_blob.table_name());
        return;
    }

    // extract or hash key ring position
    uint64_t ring_position = get_blob.has_ring_position() ?
        get_blob.ring_position() : table->ring_position(get_blob.key());

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
        _1, client, _2), get_blob.key());
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
    spb::GetBlobResponse & get_blob = *samoa_response.mutable_get_blob();

    if(!record)
    {
        get_blob.set_found(false);
        get_blob.set_version_tag("");
        client->finish_response();
        return;
    }

    get_blob.set_found(true);

    datamodel::blob::send_blob_value(client, *record,
        *get_blob.mutable_version_tag());
}

}
}
}

