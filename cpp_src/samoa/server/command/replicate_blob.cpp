
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
#include "samoa/core/protobuf_helpers.hpp"
#include "samoa/datamodel/blob.hpp"
#include "samoa/datamodel/clock_util.hpp"
#include <boost/lexical_cast.hpp>
#include <boost/bind.hpp>

namespace samoa {
namespace server {
namespace command {

namespace spb = samoa::core::protobuf;

void replicate_blob_handler::handle(const client::ptr_t & client)
{
    const spb::SamoaRequest & samoa_request = client->get_request();
    const spb::ReplicationRequest & repl_request = samoa_request.replication();

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

    // extract table & partition UUID's from raw bytes
    core::uuid table_uuid, partition_uuid;
    {
        const std::string & t_s = repl_request.table_uuid();
        const std::string & p_s = repl_request.partition_uuid();

        SAMOA_ASSERT(t_s.size() == table_uuid.size());
        std::copy(t_s.begin(), t_s.end(), table_uuid.begin());

        SAMOA_ASSERT(p_s.size() == partition_uuid.size());
        std::copy(p_s.begin(), p_s.end(), partition_uuid.begin());
    }

    table::ptr_t table = cluster_state->get_table_set()->get_table(table_uuid);

    if(!table)
    {
        client->send_error(404, "table "
            + boost::lexical_cast<std::string>(table_uuid));
        return;
    }

    local_partition::ptr_t partition = \
        boost::dynamic_pointer_cast<local_partition>(
            table->get_partition(partition_uuid));

    if(!partition)
    {
        client->send_error(404, "local partition "
            + boost::lexical_cast<std::string>(partition_uuid));
        return;
    }

    uint64_t ring_position = table->ring_position(repl_request.key());

    if(ring_position < partition->get_range_begin() ||
       ring_position > partition->get_range_end())
    {
        client->send_error(409, "wrong ring position");
        return;
    }

    // parse the replicated record
    spb::PersistedRecord_ptr_t repl_record = \
        boost::make_shared<spb::PersistedRecord>();

    core::zero_copy_input_adapter zc_adapter;
    zc_adapter.reset(client->get_request_data_blocks()[0]);

    if(!repl_record->ParseFromZeroCopyStream(&zc_adapter))
    {
        client->send_error(400, "failed to parse PersistedRecord message");
        return;
    }

    // BLOB SPECIFIC HERE

    SAMOA_ASSERT(repl_record->blob_value_size());

    datamodel::clock_util::prune_record(repl_record,
        table->get_consistency_horizon());

    partition->get_persister()->put(
        boost::bind(&replicate_blob_handler::on_put_record,
            shared_from_this(), _1, client, repl_record, _2),
        boost::bind(&replicate_blob_handler::on_merge_record,
            shared_from_this(), client,
            table->get_consistency_horizon(), _1, _2),
        repl_request.key(), repl_record);
}

spb::PersistedRecord_ptr_t replicate_blob_handler::on_merge_record(
    const client::ptr_t & client,
    unsigned consistency_horizon,
    const spb::PersistedRecord_ptr_t & local_record,
    const spb::PersistedRecord_ptr_t & repl_record)
{
    // either record may not actually have a cluster_clock,
    //  in which case it uses the default (empty) instance
    datamodel::clock_util::clock_ancestry ancestry = \
        datamodel::clock_util::compare(
            local_record->cluster_clock(), repl_record->cluster_clock(),
            consistency_horizon);

    if(ancestry == datamodel::clock_util::EQUAL)
    {
        // send empty response & abort the write
        client->finish_response();
        return spb::PersistedRecord_ptr_t();
    }
    else if(ancestry == datamodel::clock_util::MORE_RECENT)
    {
        // send local_record & abort the write
        send_record_response(client, local_record);
        return spb::PersistedRecord_ptr_t();
    }
    else if(ancestry == datamodel::clock_util::LESS_RECENT)
    {
        // remote is more recent; replace local
        return repl_record;
    }

    SAMOA_ASSERT(ancestry == datamodel::clock_util::DIVERGE);

    // merge local & repl clocks into local_record
    std::unique_ptr<spb::ClusterClock> local_clock(
        local_record->release_cluster_clock());

    datamodel::clock_util::compare(
        *local_clock.get(), repl_record->cluster_clock(),
        consistency_horizon, local_record->mutable_cluster_clock());

    // merge repl_record's blob_value into local_record
    for(int i = 0; i != repl_record->blob_value_size(); ++i)
    {
        local_record->add_blob_value(repl_record->blob_value(i));
    }

    return local_record;
}

void replicate_blob_handler::on_put_record(
    const boost::system::error_code & ec,
    const client::ptr_t & client,
    const spb::PersistedRecord_ptr_t & repl_record,
    const spb::PersistedRecord_ptr_t & put_record)
{
    if(ec)
    {
        client->send_error(500, ec);
        return;
    }

    if(put_record != repl_record)
    {
        // we performed a merge; send put_record
        send_record_response(client, put_record);
    }
    else
    {
        // remote is up-to-date; send empty response
        client->finish_response();
    }
}

void replicate_blob_handler::send_record_response(
    const client::ptr_t & client,
    const spb::PersistedRecord_ptr_t & record)
{
    core::zero_copy_output_adapter zc_adapter;

    record->SerializeToZeroCopyStream(&zc_adapter);

    client->get_response().add_data_block_length(zc_adapter.ByteCount());
    client->start_response();

    client->write_interface().queue_write(zc_adapter.output_regions());
    client->finish_response();
}

}
}
}

