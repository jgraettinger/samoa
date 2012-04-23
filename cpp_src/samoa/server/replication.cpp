
#include "samoa/server/replication.hpp"
#include "samoa/server/peer_set.hpp"
#include "samoa/server/local_partition.hpp"
#include "samoa/server/partition.hpp"
#include "samoa/server/table.hpp"
#include "samoa/persistence/persister.hpp"
#include "samoa/request/request_state.hpp"
#include "samoa/core/protobuf/zero_copy_output_adapter.hpp"
#include "samoa/core/protobuf/zero_copy_input_adapter.hpp"
#include "samoa/core/murmur_hash.hpp"
#include "samoa/error.hpp"
#include "samoa/log.hpp"
#include <functional>

namespace samoa {
namespace server {

replication::replication(
    replication::peer_request_callback_t request_callback,
    replication::peer_response_callback_t response_callback,
    request::state::ptr_t rstate)
 :  _request_callback(std::move(request_callback)),
    _response_callback(std::move(response_callback)),
    _rstate(std::move(rstate))
{ }

void replication::replicate(
    replication::peer_request_callback_t request_callback,
    replication::peer_response_callback_t response_callback,
    request::state::ptr_t rstate)
{
    ptr_t self = boost::make_shared<replication>(
        std::move(request_callback),
        std::move(response_callback),
        std::move(rstate));

    for(const auto & partition : self->_rstate->get_peer_partitions())
    {
        self->_rstate->get_peer_set()->schedule_request(
            // wrap with request's io-service to synchronize callbacks
            self->_rstate->get_io_service()->wrap(
                std::bind(&replication::on_request, self,
                    std::placeholders::_1,
                    std::placeholders::_2,
                    partition)),
                partition->get_server_uuid());
    }
}

/* static */
void replication::on_request(
    ptr_t self,
    boost::system::error_code ec,
    samoa::client::server_request_interface iface,
    partition::ptr_t partition)
{
    if(ec)
    {
        LOG_WARN("server " << partition->get_server_uuid() << ", partition " \
            << partition->get_uuid() << ": " << ec.message());

        self->_response_callback(ec,
            samoa::client::server_response_interface(), partition);
        return;
    }

    if(!self->_request_callback(iface, partition))
    {
        iface.abort_request();
        return;
    }

    self->build_peer_request(iface, partition->get_uuid());

    // reference io_service, prior to moving 'self'
    const core::io_service_ptr_t & io_srv = self->_rstate->get_io_service();

    iface.flush_request(
        // wrap with request's io-service to synchronize callbacks
        io_srv->wrap(
            std::bind(&replication::on_response, std::move(self),
                std::placeholders::_1,
                std::placeholders::_2,
                std::move(partition))));
}

/* static */
void replication::on_response(
    ptr_t self,
    boost::system::error_code ec,
    samoa::client::server_response_interface iface,
    partition::ptr_t partition)
{
    if(ec)
    {
        LOG_WARN("server " << partition->get_server_uuid() << \
            ", partition " << partition->get_uuid() << \
            ": " << ec.message());
        self->_response_callback(ec,
            samoa::client::server_response_interface(), partition);
        return;
    }

    if(iface.get_error_code())
    {
        LOG_WARN("server " << partition->get_server_uuid() << \
            " remote error on replication: " << \
            iface.get_message().error().ShortDebugString());

        if(iface.get_error_code() == 404)
        {
            // cluster-state may be inconsistent
            self->_rstate->get_peer_set()->begin_peer_discovery(
                partition->get_server_uuid());
        }

        ec = boost::asio::error::invalid_argument;
    }

    self->_response_callback(ec, iface, partition);
    iface.finish_response();
}

void replication::build_peer_request(
    const samoa::client::server_request_interface & iface,
    const core::uuid & part_uuid)
{
    spb::SamoaRequest & samoa_request = iface.get_message();

    samoa_request.set_type(spb::REPLICATE);
    samoa_request.set_key(_rstate->get_key());

    // we're handling fanout, the peer doesn't need to
    samoa_request.set_requested_quorum(1);

    samoa_request.mutable_table_uuid()->assign(
        std::begin(_rstate->get_table_uuid()),
        std::end(_rstate->get_table_uuid()));

    // assign remote partition as primary partition_uuid
    samoa_request.mutable_partition_uuid()->assign(
        std::begin(part_uuid), std::end(part_uuid));

    // assign our local partition as a peer_partition_uuid
    samoa_request.add_peer_partition_uuid()->assign(
        std::begin(_rstate->get_primary_partition_uuid()),
        std::end(_rstate->get_primary_partition_uuid()));

    // assign _other_ remote partitions as peer_partition_uuid
    for(const core::uuid & other_uuid : _rstate->get_peer_partition_uuids())
    {
        if(other_uuid != part_uuid)
        {
            samoa_request.add_peer_partition_uuid()->assign(
                std::begin(other_uuid), std::end(other_uuid));
        }
    }
}

}
}

