
#include "samoa/server/command/replicate_blob.hpp"
#include "samoa/server/client.hpp"
#include "samoa/server/cluster_state.hpp"
#include "samoa/server/context.hpp"
#include "samoa/server/table_set.hpp"
#include "samoa/server/table.hpp"
#include "samoa/server/partition.hpp"
#include "samoa/server/local_partition.hpp"
#include "samoa/persistence/persister.hpp"
#include "samoa/core/protobuf/fwd.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include "samoa/core/protobuf_helpers.hpp"
#include "samoa/datamodel/clock_util.hpp"
#include <boost/bind.hpp>

namespace samoa {
namespace server {
namespace command {

namespace spb = samoa::core::protobuf;

void replicate_blob_handler::replicate(
    const client::ptr_t & client,
    const table::ptr_t & table,
    const local_partition::ptr_t & partition,
    const std::string & key,
    const spb::PersistedRecord_ptr_t & repl_record)
{
    SAMOA_ASSERT(repl_record->blob_value_size());

    datamodel::clock_util::prune_record(repl_record,
        table->get_consistency_horizon());

    partition->get_persister()->put(
        boost::bind(&replicate_blob_handler::on_put_record,
            shared_from_this(), _1, client, repl_record, _2),
        boost::bind(&replicate_blob_handler::on_merge_record,
            shared_from_this(), client,
            table->get_consistency_horizon(), _1, _2),
        key, repl_record);
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

}
}
}

