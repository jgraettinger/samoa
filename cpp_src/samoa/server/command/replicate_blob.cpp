
#include "samoa/server/command/replicate_blob.hpp"
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
#include "samoa/datamodel/clock_util.hpp"
#include <boost/iostreams/device/array.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/bind.hpp>

namespace samoa {
namespace server {
namespace command {

namespace spb = samoa::core::protobuf;

void replicate_blob_handler::handle(const client::ptr_t & client)
{
    const spb::SamoaRequest & samoa_request = client->get_request();
    const spb::BlobRequest & repl_request = samoa_request.replication();

    if(!samoa_request.has_blob())
    {
        client->send_error(400, "expected replication field");
        return;
    }

    if(client->get_request_data_blocks().size() != 1)
    {
        client->send_error(400, "expected exactly one data block");
        return;
    }

    cluster_state::ptr_t cluster_state = \
        client->get_context()->get_cluster_state();

    core::uuid table_uuid(repl_request.table_uuid().begin(),
        repl_request.table_uuid().end());
    core::uuid partition_uuid(repl_request.partition_uuid().begin(),
        repl_request.partition_uuid().end());

    table::ptr_t table = cluster_state->get_table_set()->get_table(table_uuid);

    if(!table)
    {
        client->send_error(404, "table " + table_uuid);
        return;
    }

    local_partition::ptr_t partition = \
        boost::dynamic_pointer_cast<local_partition>(
            table->get_partition(partition_uuid));

    if(!partition)
    {
        client->send_error(404, "local partition " + partition_uuid);
        return;
    }

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
        client->send_error(503, "no available peer for request forwarding");
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

    spb::PersistedRecord_ptr_t new_rec = \
        boost::make_shared<spb::PersistedRecord>();

    // assume the key doesn't exist; initial clock tick for first write
    datamodel::clock_util::tick(*new_rec->mutable_cluster_clock(),
        primary_partition->get_uuid());

    // assign client's value
    new_rec->add_blob_value()->assign(
        boost::asio::buffers_begin(client->get_request_data_blocks()[0]),
        boost::asio::buffers_end(client->get_request_data_blocks()[0]));

    local_primary.get_persister()->put(
        boost::bind(&replicate_blob_handler::on_put_record, shared_from_this(),
            _1, client, primary_partition, all_partitions, _2),
        boost::bind(&replicate_blob_handler::on_merge_record, shared_from_this(),
            client, primary_partition, _1, _2),
        blob_request.key(), new_rec);
}

spb::PersistedRecord_ptr_t replicate_blob_handler::on_merge_record(
    const client::ptr_t & client,
    const partition::ptr_t & primary_partition,
    const spb::PersistedRecord_ptr_t & cur_rec,
    const spb::PersistedRecord_ptr_t & new_rec)
{
    const spb::SamoaRequest & samoa_request = client->get_request();
    const spb::BlobRequest & blob_request = samoa_request.blob();

    spb::SamoaResponse & samoa_response = client->get_response();
    spb::BlobResponse & blob_response = *samoa_response.mutable_blob();

    // if request included a cluster clock, validate
    //  equality against the stored cluster clock
    if(blob_request.has_cluster_clock())
    {
        if(datamodel::clock_util::compare(
                cur_rec->cluster_clock(),
                blob_request.cluster_clock() 
            ) != datamodel::clock_util::EQUAL)
        {
            // clock doesn't match: abort
            blob_response.set_success(false);
            datamodel::blob::send_blob_value(client, *cur_rec);
            return spb::PersistedRecord_ptr_t();
        }
    }

    // clocks match, or the request doesn't have a clock (implicit match)
    //   copy the current clock, and tick to reflect this operation 
    new_rec->mutable_cluster_clock()->CopyFrom(cur_rec->cluster_clock());
    datamodel::clock_util::tick(*new_rec->mutable_cluster_clock(),
        primary_partition->get_uuid());

    return new_rec;
}

void replicate_blob_handler::on_put_record(
    const boost::system::error_code & ec,
    const client::ptr_t & client,
    const partition::ptr_t & primary_partition,
    const table::ring_t & all_partitions,
    const spb::PersistedRecord_ptr_t & new_rec)
{
    if(ec)
    {
        client->send_error(500, ec);
        return;
    }

    spb::SamoaResponse & samoa_response = client->get_response();
    spb::BlobResponse & blob_response = *samoa_response.mutable_blob();

    blob_response.set_success(true);
    client->finish_response();

    // TODO: post replication operations to remaining partitions 

    return;
}

}
}
}

