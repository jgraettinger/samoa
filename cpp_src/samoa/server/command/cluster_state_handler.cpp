
#include "samoa/server/command/cluster_state_handler.hpp"
#include "samoa/server/cluster_state.hpp"
#include "samoa/server/client.hpp"
#include "samoa/server/context.hpp"
#include "samoa/server/request_state.hpp"
#include "samoa/log.hpp"
#include <boost/bind.hpp>

namespace samoa {
namespace server {
namespace command {

namespace spb = samoa::core::protobuf;

void cluster_state_handler::handle(const request_state::ptr_t & rstate)
{
    if(rstate->get_request_data_blocks().empty())
    {
        on_complete(rstate);
    }
    else if(rstate->get_request_data_blocks().size() != 1)
    {
        rstate->send_client_error(400, "expected exactly one data block");
    }
    else
    {
        rstate->get_context()->cluster_state_transaction(
            boost::bind(&cluster_state_handler::on_state_transaction,
                shared_from_this(), _1, rstate));
    }
}

bool cluster_state_handler::on_state_transaction(spb::ClusterState & local_state,
    const request_state::ptr_t & rstate)
{
    core::zero_copy_input_adapter zci_adapter(
        rstate->get_request_data_blocks()[0]);

    spb::ClusterState remote_state;
    SAMOA_ASSERT(remote_state.ParseFromZeroCopyStream(&zci_adapter));

    rstate->get_io_service()->post(
        boost::bind(&cluster_state_handler::on_complete,
            shared_from_this(), rstate));

    return rstate->get_context()->get_cluster_state(
        )->merge_cluster_state(remote_state, local_state);
}

void cluster_state_handler::on_complete(const request_state::ptr_t & rstate)
{
    core::zero_copy_output_adapter zco_adapter;

    SAMOA_ASSERT(rstate->get_context()->get_cluster_state(
        )->get_protobuf_description().SerializeToZeroCopyStream(&zco_adapter));

    rstate->get_samoa_response().add_data_block_length(zco_adapter.ByteCount());

    rstate->start_client_response();
    rstate->get_client()->write_interface().queue_write(
        zco_adapter.output_regions());

    rstate->finish_client_response();
}

}
}
}

