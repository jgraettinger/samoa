
#include "samoa/server/command/basic_replicate.hpp"
#include "samoa/server/client.hpp"
#include "samoa/server/cluster_state.hpp"
#include "samoa/server/context.hpp"
#include "samoa/server/table_set.hpp"
#include "samoa/server/table.hpp"
#include "samoa/server/partition.hpp"
#include "samoa/server/local_partition.hpp"
#include "samoa/core/protobuf/fwd.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include "samoa/core/protobuf_helpers.hpp"
#include <boost/lexical_cast.hpp>
#include <boost/bind.hpp>

namespace samoa {
namespace server {
namespace command {

namespace spb = samoa::core::protobuf;

basic_replicate_handler::~basic_replicate_handler()
{ }

void basic_replicate_handler::handle(const client::ptr_t & client)
{
    const spb::SamoaRequest & samoa_request = client->get_request();
    const spb::ReplicationRequest & repl_request = samoa_request.replication();

    if(!samoa_request.has_replication())
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
        const std::string & p_s = repl_request.target_partition_uuid();

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

    if(!partition->position_in_responsible_range(ring_position))
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

    // pass off to derived class for datatype-sepecific handling
    replicate(client, table, partition, repl_request.key(), repl_record);
}

void basic_replicate_handler::replication_complete(
    const client::ptr_t & client,
    const table::ptr_t & table,
    bool was_updated,
    bool still_divergent,
    const spb::PersistedRecord_ptr_t & stored_record)
{
    const spb::SamoaRequest & samoa_request = client->get_request();
    const spb::ReplicationRequest & repl_request = samoa_request.replication();

    spb::ReplicationResponse & repl_response = *client->get_response(
        ).mutable_replication();

    repl_response.set_updated(was_updated);
    repl_response.set_divergent(still_divergent);
    client->finish_response();
    
    const std::string & p_s = repl_request.target_partition_uuid();

    SAMOA_ASSERT(t_s.size() == table_uuid.size());
    std::copy(t_s.begin(), t_s.end(), table_uuid.begin());

    SAMOA_ASSERT(p_s.size() == partition_uuid.size());
    std::copy(p_s.begin(), p_s.end(), partition_uuid.begin());

    table::ring_route route;
    route.primary_partition = 

    replication_operation::spawn_replication(
        client->get_context()->get_cluster_state()->get_peer_set(),
        table, repl_request.key(), 


}

void basic_replicate_handler::on_put_record(
    const boost::system::error_code & ec,
    const client::ptr_t & client,
    const table::ptr_t & table,
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
        // we performed a merge
        replication_complete(client, true, true, put_record);
    }
    else
    {
        // we performed a first write, or overwrote stale content
        replication_complete(client, true, false, put_record);
    }
}

}
}
}

