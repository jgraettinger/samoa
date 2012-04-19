
#include "samoa/server/peer_discovery.hpp"
#include "samoa/server/context.hpp"
#include "samoa/server/cluster_state.hpp"
#include "samoa/server/peer_set.hpp"
#include "samoa/core/protobuf/zero_copy_output_adapter.hpp"
#include "samoa/core/protobuf/zero_copy_input_adapter.hpp"
#include "samoa/error.hpp"
#include "samoa/log.hpp"
#include <boost/asio.hpp>
#include <functional>

namespace samoa {
namespace server {

namespace spb = samoa::core::protobuf;

peer_discovery::peer_discovery(const context::ptr_t & context,
    const core::uuid & peer_uuid)
 : _weak_context(context->shared_from_this()), // pointer-from-python guard
   _peer_uuid(peer_uuid)
{
}

void peer_discovery::operator()()
{
    auto noop_callback = [](const boost::system::error_code &) {};
    this->operator()(noop_callback);
}

void peer_discovery::operator()(callback_t && callback)
{
    {
    	spinlock::guard guard(_lock);

    	if(_context)
        {
            callback(boost::system::errc::make_error_code(
                boost::system::errc::operation_in_progress));
            return;
        }

        _context = _weak_context.lock();

        if(!_context)
        {
            // shutdown race condition
            callback(boost::system::errc::make_error_code(
                boost::system::errc::operation_canceled));
            return;
        }
    }

    _callback = std::move(callback);

    LOG_INFO("beginning discovery <" << _context->get_server_uuid() \
        << " => " << _peer_uuid << ">");

    peer_set::ptr_t peer_set = _context->get_cluster_state()->get_peer_set();
    if(!peer_set->has_server(_peer_uuid))
    {
        LOG_WARN("peer " << _peer_uuid << " is unknown (race condition?)");
        finish(boost::system::errc::make_error_code(
            boost::system::errc::operation_canceled));
        return;
    }

    peer_set->schedule_request(
        std::bind(&peer_discovery::on_request,
            shared_from_this(),
            std::placeholders::_1,
            std::placeholders::_2),
        _peer_uuid);
}

void peer_discovery::on_request(
    boost::system::error_code ec,
    samoa::client::server_request_interface iface)
{
    if(ec)
    {
        LOG_ERR(ec.message());
        finish(ec);
        return;
    }

    // serialize current cluster-state protobuf description;
    core::protobuf::zero_copy_output_adapter zco_adapter;
    _context->get_cluster_state()->get_protobuf_description(
        ).SerializeToZeroCopyStream(&zco_adapter);

    iface.get_message().set_type(spb::CLUSTER_STATE);
    iface.add_data_block(zco_adapter.output_regions());

    iface.flush_request(
        std::bind(&peer_discovery::on_response,
            shared_from_this(),
            std::placeholders::_1,
            std::placeholders::_2));
}

void peer_discovery::on_response(
    boost::system::error_code ec,
    samoa::client::server_response_interface iface)
{
    if(ec)
    {
        LOG_ERR(ec.message());
        finish(ec);
        return;
    }

    if(iface.get_error_code())
    {
        LOG_ERR(_peer_uuid << " discovery: " << \
            iface.get_message().error().ShortDebugString());

        iface.finish_response();
        finish(boost::system::errc::make_error_code(
            boost::system::errc::protocol_error));
        return;
    }

    // parse returned ClusterState protobuf message
    SAMOA_ASSERT(iface.get_response_data_blocks().size() == 1);

    core::protobuf::zero_copy_input_adapter zci_adapter(
        iface.get_response_data_blocks()[0]);
    SAMOA_ASSERT(_remote_state.ParseFromZeroCopyStream(&zci_adapter));

    _context->cluster_state_transaction(
        std::bind(&peer_discovery::on_state_transaction,
            shared_from_this(), std::placeholders::_1));

    iface.finish_response();
}

bool peer_discovery::on_state_transaction(
    spb::ClusterState & local_state)
{
    try
    {
        bool result = _context->get_cluster_state()->merge_cluster_state(
            _remote_state, local_state);

        finish(boost::system::error_code());
        return result;
    }
    catch(const std::exception & exc)
    {
        LOG_ERR(exc.what());
        finish(boost::system::errc::make_error_code(
            boost::system::errc::bad_message));
        throw;
    }
}

void peer_discovery::finish(const boost::system::error_code & ec)
{
    _context.reset();
    callback_t(std::move(_callback))(ec);
}

}
}

