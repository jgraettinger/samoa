
#include "samoa/server/command/digest_sync_handler.hpp"
#include "samoa/request/request_state.hpp"
#include "samoa/request/state_exception.hpp"
#include "samoa/server/remote_digest.hpp"
#include "samoa/server/cluster_state.hpp"
#include "samoa/server/table_set.hpp"
#include "samoa/server/table.hpp"
#include "samoa/server/remote_partition.hpp"
#include "samoa/server/context.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include <memory>

namespace samoa {
namespace server {
namespace command {

namespace spb = samoa::core::protobuf;

void digest_sync_handler::handle(const request::state::ptr_t & rstate)
{
    rstate->load_table_state();

    if(!rstate->get_samoa_request().has_partition_uuid())
    {
        throw request::state_exception(400,
            "expected partition_uuid");
    }

    if(!rstate->get_table()->get_partition(
        rstate->get_primary_partition_uuid()))
    {
    	throw request::state_exception(404,
    	    "no such partition");
    }

    if(rstate->get_request_data_blocks().empty())
    {
    	throw request::state_exception(400,
    	    "expected exactly one data block");
    }

    if(!rstate->get_samoa_request().has_digest_properties())
    {
        throw request::state_exception(400,
            "expected digest_properties");
    }

    remote_digest::ptr_t digest = std::make_shared<remote_digest>(
        rstate->get_context()->get_server_uuid(),
        rstate->get_primary_partition_uuid(),
        rstate->get_samoa_request().digest_properties(),
        rstate->get_request_data_blocks()[0]);

    // We want to synchronize swapping out digests with cluster state
    //  transactions which may be going on; isolate using the cluster
    //  state transaction io_service

    auto digest_transaction = [rstate, digest]()
    {
        cluster_state::ptr_t cluster_state = rstate->get_context(
            )->get_cluster_state();

        table::ptr_t table = cluster_state->get_table_set()->get_table(
            rstate->get_table_uuid());

        if(!table)
        {
            rstate->send_error(404, "no such table");
            return;
        }

        remote_partition::ptr_t partition = \
            std::dynamic_pointer_cast<remote_partition>(
                table->get_partition(rstate->get_primary_partition_uuid()));

        if(!partition)
        {
        	rstate->send_error(404, "no such partition");
        	return;
        }
        if(!partition->is_tracked())
        {
            rstate->send_error(404, "sent digest for an untracked partition");
            return;
        }

        dynamic_cast<remote_digest &>(*partition->get_digest()
            ).mark_filter_for_deletion();

        partition->set_digest(digest);
        rstate->flush_response();
    };
    rstate->get_context()->get_cluster_state_transaction_service()->dispatch(
        digest_transaction);
}

}
}
}

