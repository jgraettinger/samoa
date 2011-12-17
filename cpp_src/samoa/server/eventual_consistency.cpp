#include "samoa/server/eventual_consistency.hpp"
#include "samoa/core/proactor.hpp"

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

void eventual_consistency::operator()(
    const request::state_ptr_t & rstate)
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
        core::proactor::get_instance()->serial_io_service());
    rstate->load_context_state(ctxt);

    rstate->set_table_uuid(_table_uuid);
    rstate->load_table_state();

    // moving a key off of the local partition requires a
    //  successful quorum of all responsible remote partitions
    rstate->set_quorum_count(0);
    rstate->load_replication_state();

    // assume the common case:
    //  this key still belongs with this partition
    rstate->set_primary_partition_uuid(_partition_uuid);
    try {
        rstate->load_route_state();

        // replicate 
        rstate->peer_replication_success();

        replication::replicated_write(
            boost::bind(&eventual_consistency::on_replication,
                shared_from_this()),
            rstate);
    }
    catch(const request::state_exception & e)
    {
        // nope, this key doesn't belong on our partition;
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
}

void eventual_consistency::on_move_request(
    const boost::system::error_code & ec,
    samoa::client::server_request_interface iface,
    const persistence::persister::ptr_t & persister,
    const request::state::ptr_t & rstate)

}
}

