
#include "samoa/server/command/set_blob.hpp"
#include "samoa/server/client.hpp"
#include "samoa/server/table.hpp"
#include "samoa/server/peer_set.hpp"
#include "samoa/server/partition.hpp"
#include "samoa/server/local_partition.hpp"
#include "samoa/server/replication.hpp"
#include "samoa/persistence/persister.hpp"
#include "samoa/request/request_state.hpp"
#include "samoa/request/state_exception.hpp"
#include "samoa/core/protobuf/fwd.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include "samoa/datamodel/blob.hpp"
#include "samoa/datamodel/clock_util.hpp"
#include "samoa/log.hpp"

namespace samoa {
namespace server {
namespace command {

namespace spb = samoa::core::protobuf;
using std::begin;
using std::end;

void set_blob_handler::handle(const request::state::ptr_t & rstate)
{
    if(rstate->get_request_data_blocks().empty())
    {
        throw request::state_exception(400,
            "expected exactly one data block");
    }

    rstate->load_table_state();
    rstate->load_route_state();

    if(!rstate->get_primary_partition())
    {
        // no primary partition; forward to a better peer
        rstate->get_peer_set()->forward_request(rstate);
        return;
    }
    rstate->load_replication_state();

    // assume the key doesn't exist, and we're creating a new record
    datamodel::blob::update(rstate->get_remote_record(),
        rstate->get_primary_partition()->get_author_id(),
        rstate->get_request_data_blocks()[0]);

    auto on_merge = [rstate](
        spb::PersistedRecord & local_record,
        const spb::PersistedRecord &) -> datamodel::merge_result
    {
        datamodel::merge_result result;

        // if request included a cluster clock, validate
        //  equality against the stored cluster clock
        if(rstate->get_samoa_request().has_cluster_clock())
        {
            if(datamodel::clock_util::compare(
                    local_record.cluster_clock(),
                    rstate->get_samoa_request().cluster_clock(),
                    rstate->get_table()->get_consistency_horizon()
                ) != datamodel::clock_util::CLOCKS_EQUAL)
            {
                // clock doesn't match: abort
                return datamodel::merge_result(false, false);
            }
        }

        // clocks match, or the request doesn't have a clock (implicit match)
        //   update the record to 'repair' any divergence with the new value

        datamodel::blob::update(local_record,
            rstate->get_primary_partition()->get_author_id(),
            rstate->get_request_data_blocks()[0]);

        datamodel::blob::prune(local_record,
            rstate->get_table()->get_consistency_horizon());

        return datamodel::merge_result(true, true);
    };

    auto on_write = [rstate](
        const boost::system::error_code & ec,
        const datamodel::merge_result & merge_result)
    {
        if(ec)
        {
            rstate->send_error(500, ec);
            return;
        }

        if(!merge_result.local_was_updated)
        {
            rstate->get_samoa_response().mutable_cluster_clock()->CopyFrom(
                rstate->get_local_record().cluster_clock());

            datamodel::blob::value(rstate->get_local_record(),
                [&](const std::string & value)
                { rstate->add_response_data_block(begin(value), end(value)); }
            );
        }
        rstate->flush_response();
    };

    rstate->get_primary_partition()->write(
        on_write, on_merge, rstate, true);
}

}
}
}

