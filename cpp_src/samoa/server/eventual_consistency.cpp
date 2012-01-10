#include "samoa/server/eventual_consistency.hpp"
#include "samoa/server/context.hpp"
#include "samoa/server/replication.hpp"
#include "samoa/server/peer_set.hpp"
#include "samoa/server/table.hpp"
#include "samoa/server/local_partition.hpp"
#include "samoa/persistence/persister.hpp"
#include "samoa/request/request_state.hpp"
#include "samoa/request/state_exception.hpp"
#include "samoa/core/proactor.hpp"
#include "samoa/error.hpp"
#include "samoa/log.hpp"
#include <boost/bind.hpp>

namespace samoa {
namespace server {

eventual_consistency::eventual_consistency(
    const context::ptr_t & context,
    const core::uuid & table_uuid,
    const core::uuid & partition_uuid,
    const datamodel::prune_func_t & prune_func)
 :  _weak_context(context),
    _table_uuid(table_uuid),
    _partition_uuid(partition_uuid),
    _prune_func(prune_func)
{ }

bool eventual_consistency::operator()(
    const request::state::ptr_t & rstate)
{
    context::ptr_t ctxt = _weak_context.lock();
    if(!ctxt)
    {
        // shutdown race condition
        return true;
    }

    if(!_prune_func(rstate->get_local_record()))
    {
        // pruning indicates this record should be discarded;
        //  return to persister immediately
        return false;
    }

    rstate->load_io_service_state(
        core::proactor::get_proactor()->serial_io_service());
    rstate->load_context_state(ctxt);

    rstate->set_table_uuid(_table_uuid);
    rstate->load_table_state();

    // assume common case, that this key still belongs here
    rstate->set_primary_partition_uuid(_partition_uuid);
    try {
        rstate->load_route_state();

        // quorum is all responsible partitions
        rstate->set_quorum_count(0);
        rstate->load_replication_state();

        // replicate value to peers
        rstate->peer_replication_success();
        replication::replicated_write(
            boost::bind(&eventual_consistency::on_replication,
                shared_from_this()),
            rstate);
    }
    catch(const request::state_exception & e)
    {
        // this key doesn't belong on our partition;
        //  back off to routing by key alone, and attempt
        //  to move it to responsible partitions
        std::string key = rstate->get_key();
        rstate->reset_route_state();
        rstate->set_key(std::move(key));

        rstate->load_route_state();

        rstate->get_peer_set()->schedule_request(
            boost::bind(&eventual_consistency::on_move_request,
                shared_from_this(), _1, _2, rstate),
            rstate->get_peer_set()->select_best_peer(rstate));
    }
    return true;
}

void eventual_consistency::on_replication()
{
    // no-op
}

void eventual_consistency::on_move_request(
    const boost::system::error_code & ec,
    samoa::client::server_request_interface iface,
    const request::state::ptr_t & rstate)
{
    if(ec)
    {
    	LOG_WARN(ec.message());
    	return;
    }

    spb::SamoaRequest & samoa_request = iface.get_message();

    samoa_request.set_type(spb::REPLICATE);
    samoa_request.set_key(rstate->get_key());

    // the handling peers should perform a replication fanout
    samoa_request.set_requested_quorum(
        rstate->get_table()->get_replication_factor());

    samoa_request.mutable_table_uuid()->assign(
        rstate->get_table_uuid().begin(),
        rstate->get_table_uuid().end());

    // serialize remote record to the peer
    core::zero_copy_output_adapter zco_adapter;
    SAMOA_ASSERT(rstate->get_local_record(
        ).SerializeToZeroCopyStream(&zco_adapter));
    iface.add_data_block(zco_adapter.output_regions());

    iface.flush_request(
        boost::bind(&eventual_consistency::on_move_response,
            shared_from_this(), _1, _2, rstate));
}

void eventual_consistency::on_move_response(
    const boost::system::error_code & ec,
    samoa::client::server_response_interface iface,
    const request::state::ptr_t & rstate)
{
    if(ec)
    {
    	LOG_WARN(ec.message());
    	return;
    }
    if(iface.get_error_code())
    {
        LOG_WARN("remote error on move: " << \
            iface.get_message().error().ShortDebugString());

        iface.finish_response();
        return;
    }
    if(iface.get_message().replication_success() != \
        rstate->get_table()->get_replication_factor())
    {
        LOG_WARN("failed to meet quorum while moving " << \
            log::ascii_escape(rstate->get_key()));

        iface.finish_response();
        return;
    }

    local_partition::ptr_t partition = \
        boost::dynamic_pointer_cast<local_partition>(
            rstate->get_table()->get_partition(_partition_uuid));
    SAMOA_ASSERT(partition);

    partition->get_persister()->drop(
        boost::bind(&eventual_consistency::on_move_drop,
            shared_from_this(), _1, rstate),
        rstate->get_key(), rstate->get_local_record());
}

bool eventual_consistency::on_move_drop(bool found,
    const request::state_ptr_t & /* guard */)
{
    SAMOA_ASSERT(found);
    return true;
}

}
}

