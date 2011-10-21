
#include "samoa/server/command/get_blob.hpp"
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
#include <boost/bind.hpp>

namespace samoa {
namespace server {
namespace command {

namespace spb = samoa::core::protobuf;

void get_blob_handler::handle(const request::state::ptr_t & rstate)
{
    try {
        rstate->load_table_state();
        rstate->load_route_state();

        if(!rstate->get_primary_partition())
        {
            // no primary partition; forward to a better peer
            rstate->get_peer_set()->forward_request(rstate);
            return;
        }

        rstate->load_replication_state();
    }
    catch (const request::state_exception & ex)
    {
        rstate->send_error(ex.get_code(), ex.what());
        return;
    }

    // optimistically assume local read will succeed
    rstate->peer_replication_success();

    if(rstate->get_quorum_count() > 1)
    {
        // spawn replicated read requests to remaining peers
        replication::replicated_read(
            boost::bind(&get_blob_handler::on_replicated_read,
                shared_from_this(), rstate),
            rstate);
    }
    else
    {
        // simple case: start a persister read into local-record
        rstate->get_primary_partition()->get_persister()->get(
            boost::bind(&get_blob_handler::on_retrieve,
                shared_from_this(), _1, rstate),
            rstate->get_key(),
            rstate->get_local_record());
    }
}

void get_blob_handler::on_replicated_read(
    const request::state::ptr_t & rstate)
{
    // did the replicated read return anything?
    if(rstate->get_remote_record().has_cluster_clock() ||
       rstate->get_remote_record().blob_value_size())
    {
        // speculatively write the merged remote record; as a side-effect,
        //  local-record will be populated with the merged result
        rstate->get_primary_partition()->get_persister()->put(
            boost::bind(&get_blob_handler::on_retrieve,
                shared_from_this(), _1, rstate),
            datamodel::merge_func_t(
                rstate->get_table()->get_consistent_merge()),
            rstate->get_key(),
            rstate->get_remote_record(),
            rstate->get_local_record());
    }
    else
    {
        // no populated peer records exist; fall back to a persister read
        rstate->get_primary_partition()->get_persister()->get(
            boost::bind(&get_blob_handler::on_retrieve,
                shared_from_this(), _1, rstate),
            rstate->get_key(),
            rstate->get_local_record());
    }
}

void get_blob_handler::on_retrieve(
    const boost::system::error_code & ec,
    const request::state::ptr_t & rstate)
{
    if(ec)
    {
        rstate->send_error(504, ec);
        return;
    }

    rstate->get_samoa_response().set_replication_success(
        rstate->get_peer_success_count());
    rstate->get_samoa_response().set_replication_failure(
        rstate->get_peer_failure_count());

    datamodel::blob::send_blob_value(rstate,
        rstate->get_local_record());
}

}
}
}

