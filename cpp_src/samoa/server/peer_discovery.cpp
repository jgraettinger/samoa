
#include "samoa/server/peer_discovery.hpp"
#include "samoa/server/context.hpp"
#include "samoa/server/cluster_state.hpp"
#include "samoa/server/peer_set.hpp"
#include "samoa/core/tasklet_group.hpp"
#include "samoa/core/protobuf_helpers.hpp"
#include "samoa/error.hpp"
#include "samoa/log.hpp"
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <sstream>

namespace samoa {
namespace server {

namespace spb = samoa::core::protobuf;

// default period of 1 minute
unsigned default_period_ms = 60 * 1000;

peer_discovery::peer_discovery(const context::ptr_t & context,
    const core::uuid & peer_uuid)
 : periodic_task<peer_discovery>(context, default_period_ms),
   _peer_uuid(peer_uuid)
{
    LOG_DBG(peer_uuid);

    {
        std::stringstream tmp;
        tmp << "peer_discovery<" << this << ">@" << peer_uuid;
        set_tasklet_name(tmp.str());
    }
}

void peer_discovery::begin_iteration(const context::ptr_t & context)
{
    LOG_DBG("called " << get_tasklet_name());

    if(!context->get_cluster_state()->get_peer_set()->has_server(_peer_uuid))
    {
        LOG_ERR("peer is unknown: " << _peer_uuid);
        end_iteration();
        return;
    }

    context->get_cluster_state()->get_peer_set()->schedule_request(
        boost::bind(&peer_discovery::on_request,
            shared_from_this(), _1, _2, context),
        _peer_uuid);
}

void peer_discovery::on_request(
    const boost::system::error_code & ec,
    samoa::client::server_request_interface & iface,
    const context::ptr_t & context)
{
    if(ec)
    {
        LOG_ERR(ec.message());
        end_iteration();
        return;
    }

    // serialize current cluster-state protobuf description;
    core::zero_copy_output_adapter zco_adapter;
    context->get_cluster_state()->get_protobuf_description(
        ).SerializeToZeroCopyStream(&zco_adapter);

    iface.get_message().set_type(spb::CLUSTER_STATE);
    iface.add_data_block(zco_adapter.output_regions());

    iface.flush_request(
        boost::bind(&peer_discovery::on_response,
            shared_from_this(), _1, _2, context));
}

void peer_discovery::on_response(
    const boost::system::error_code & ec,
    samoa::client::server_response_interface & iface,
    const context::ptr_t & context)
{
    if(ec)
    {
        LOG_ERR(ec.message());
        end_iteration();
        return;
    }

    if(iface.get_error_code())
    {
        LOG_ERR(_peer_uuid << " discovery: " << \
            iface.get_message().error().ShortDebugString());

        iface.finish_response();
        end_iteration();
        return;
    }

    // parse returned ClusterState protobuf message
    SAMOA_ASSERT(iface.get_response_data_blocks().size() == 1);

    core::zero_copy_input_adapter zci_adapter(
        iface.get_response_data_blocks()[0]);
    SAMOA_ASSERT(_remote_state.ParseFromZeroCopyStream(&zci_adapter));

    context->cluster_state_transaction(
        boost::bind(&peer_discovery::on_state_transaction,
            shared_from_this(), _1, context));

    iface.finish_response();
}

bool peer_discovery::on_state_transaction(spb::ClusterState & local_state,
    const context::ptr_t & context)
{
    try
    {
        bool result = context->get_cluster_state()->merge_cluster_state(
            _remote_state, local_state);

        end_iteration();
        return result;
    }
    catch(...)
    {
        end_iteration();
        throw;
    }
}

}
}

