
#include "samoa/server/command/set_blob.hpp"
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
#include <boost/iostreams/device/array.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/bind.hpp>

namespace samoa {
namespace server {
namespace command {

namespace bio = boost::iostreams;
namespace spb = samoa::core::protobuf;

void set_blob_handler::handle(const client::ptr_t & client)
{
    const spb::SamoaRequest & samoa_request = client->get_request();
    const spb::SetBlobRequest & set_blob = samoa_request.set_blob();

    if(!samoa_request.has_set_blob())
    {
        client->send_error(400, "expected set_blob");
        return;
    }

    if(client->get_request_data_blocks().size() != 1)
    {
        client->send_error(400, "expected exactly one data block");
        return;
    }

    cluster_state::ptr_t cluster_state = \
        client->get_context()->get_cluster_state();

    table::ptr_t table = cluster_state->get_table_set()->get_table_by_name(
        set_blob.table_name());

    if(!table)
    {
        client->send_error(404, "table " + set_blob.table_name());
        return;
    }

    // extract or hash the key's ring position
    uint64_t ring_position = set_blob.has_ring_position() ?
        set_blob.ring_position() : table->ring_position(set_blob.key());

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

    size_t expected_length = datamodel::blob::serialized_length(
        samoa_request.data_block_length(0));

    local_primary.get_persister()->put(
        boost::bind(&set_blob_handler::on_put_record, shared_from_this(),
            _1, client, primary_partition, all_partitions, _2, _3),
        set_blob.key(), expected_length);
}

bool set_blob_handler::on_put_record(
    const boost::system::error_code & ec,
    const client::ptr_t & client,
    const partition::ptr_t & primary_partition,
    const table::ring_t & all_partitions,
    const persistence::record * cur_record,
    persistence::record * new_record)
{
    if(ec)
    {
        client->send_error(500, ec);
        return false;
    }

    const spb::SamoaRequest & samoa_request = client->get_request();
    const spb::SetBlobRequest & set_blob_req = samoa_request.set_blob();

    spb::SamoaResponse & samoa_response = client->get_response();
    spb::SetBlobResponse & set_blob_resp = *samoa_response.mutable_set_blob();

    datamodel::cluster_clock clock;

    if(cur_record)
    {
        // a record exists for this key; extract it's clock
        bio::stream<bio::array_source> istr(
            cur_record->value_begin(), cur_record->value_end());

        istr >> clock;

        // if the client specified a version clock, check
        //  equality against the record's clock.
        if(set_blob_req.has_version_tag())
        {
            datamodel::cluster_clock request_clock;
            std::stringstream(set_blob_req.version_tag()) >> request_clock;

            if(request_clock != clock)
            {
                set_blob_resp.set_success(false);

                // return the current version clock & blob value
                datamodel::blob::send_blob_value(client,
                    *cur_record, *set_blob_resp.mutable_version_tag());

                // abort the record put
                return false;
            }
        }

        // if the client didn't give a version clock, it's implicitly
        //  'equal' to the current clock; existing values are clobbered
    }

    // tick clock to reflect this operation
    clock.tick(primary_partition->get_uuid());

    // TODO: Prune clock?

    // write new blob record value
    datamodel::blob::write_blob_value(
        clock, client->get_request_data_blocks()[0], *new_record); 

    set_blob_resp.set_success(true);
    client->finish_response();

    // TODO: post replication operations to remaining partitions 

    // commit the new record
    return true;
}

}
}
}

