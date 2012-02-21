
#include "samoa/server/command/update_counter.hpp"
#include "samoa/server/client.hpp"
#include "samoa/server/table.hpp"
#include "samoa/server/peer_set.hpp"
#include "samoa/server/partition.hpp"
#include "samoa/server/local_partition.hpp"
#include "samoa/persistence/persister.hpp"
#include "samoa/request/request_state.hpp"
#include "samoa/request/state_exception.hpp"
#include "samoa/core/protobuf/fwd.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include "samoa/datamodel/counter.hpp"
#include "samoa/datamodel/clock_util.hpp"
#include "samoa/log.hpp"
#include <boost/bind.hpp>

namespace samoa {
namespace server {
namespace command {

namespace spb = samoa::core::protobuf;

void update_counter_handler::handle(const request::state::ptr_t & rstate)
{
    if(!rstate->get_samoa_request().has_counter_update())
    {
        throw request::state_exception(400,
            "expected counter_update to be set");
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
    datamodel::counter::update(rstate->get_remote_record(),
        rstate->get_primary_partition()->get_author_id(),
        rstate->get_samoa_request().counter_update());

    auto on_merge = [rstate](
        spb::PersistedRecord & local_record,
        const spb::PersistedRecord &) -> datamodel::merge_result
    {
        // the key already exists; update and prune the present counter
        datamodel::counter::update(local_record,
            rstate->get_primary_partition()->get_author_id(),
            rstate->get_samoa_request().counter_update());

        datamodel::counter::prune(local_record,
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

        SAMOA_ASSERT(merge_result.local_was_updated);

        // return updated value to caller
        spb::SamoaResponse & response = rstate->get_samoa_response();
        response.set_counter_value(
            datamodel::counter::value(rstate->get_local_record()));

        rstate->flush_response();
    };

    rstate->get_primary_partition()->write(
        on_write, on_merge, rstate, true);
}

}
}
}

