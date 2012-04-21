
#include "samoa/server/command/cluster_state_handler.hpp"
#include "samoa/server/cluster_state.hpp"
#include "samoa/server/client.hpp"
#include "samoa/server/context.hpp"
#include "samoa/request/request_state.hpp"
#include "samoa/core/protobuf/zero_copy_output_adapter.hpp"
#include "samoa/core/protobuf/zero_copy_input_adapter.hpp"
#include "samoa/log.hpp"
#include <functional>

namespace samoa {
namespace server {
namespace command {

namespace spb = samoa::core::protobuf;

void cluster_state_handler::handle(const request::state::ptr_t & rstate)
{
    if(rstate->get_request_data_blocks().empty())
    {
        on_complete(rstate);
    }
    else if(rstate->get_request_data_blocks().size() != 1)
    {
        rstate->send_error(400, "expected exactly one data block");
    }
    else
    {
        rstate->get_context()->cluster_state_transaction(
            std::bind(&cluster_state_handler::on_state_transaction,
                shared_from_this(), std::placeholders::_1, rstate));
    }
}

bool cluster_state_handler::on_state_transaction(
    spb::ClusterState & local_state,
    const request::state::ptr_t & rstate)
{
    core::protobuf::zero_copy_input_adapter zci_adapter(
        rstate->get_request_data_blocks()[0]);

    spb::ClusterState remote_state;
    SAMOA_ASSERT(remote_state.ParseFromZeroCopyStream(&zci_adapter));

    rstate->get_io_service()->post(
        std::bind(&cluster_state_handler::on_complete,
            shared_from_this(), rstate));

    return rstate->get_context()->get_cluster_state(
        )->merge_cluster_state(remote_state, local_state);
}

void cluster_state_handler::on_complete(const request::state::ptr_t & rstate)
{
    core::protobuf::zero_copy_output_adapter zco_adapter;

    // respond with the current cluster-state, rather than the request::state's
    SAMOA_ASSERT(rstate->get_context()->get_cluster_state(
        )->get_protobuf_description().SerializeToZeroCopyStream(&zco_adapter));

    rstate->add_response_data_block(zco_adapter.output_regions());
    rstate->flush_response();
}

}
}
}

