
#include "samoa/server/replication.hpp"
#include "samoa/server/peer_set.hpp"
#include "samoa/server/local_partition.hpp"
#include "samoa/server/partition.hpp"
#include "samoa/server/table.hpp"
#include "samoa/server/digest.hpp"
#include "samoa/persistence/persister.hpp"
#include "samoa/request/request_state.hpp"
#include "samoa/core/protobuf/zero_copy_output_adapter.hpp"
#include "samoa/core/protobuf/zero_copy_input_adapter.hpp"
#include "samoa/core/murmur_hash.hpp"
#include "samoa/error.hpp"
#include "samoa/log.hpp"
#include <boost/bind.hpp>

namespace samoa {
namespace server {

void replication::replicate(
    const replication::peer_request_callback_t & request_callback,
    const replication::peer_response_callback_t & response_callback,
    const request::state_ptr_t & rstate)
{

    for(const partition::ptr_t & partition : rstate->get_peer_partitions())
    {
        rstate->get_peer_set()->schedule_request(
            // wrap with request's io-service to synchronize callbacks
            rstate->get_io_service()->wrap(
                boost::bind(&replication::on_request, _1, _2,
                    request_callback, response_callback, rstate, partition)),
                partition->get_server_uuid());
    }
}

void replication::on_request(
    const boost::system::error_code & ec,
    samoa::client::server_request_interface iface,
    const replication::peer_request_callback_t & request_callback,
    const replication::peer_response_callback_t & response_callback,
    const request::state::ptr_t & rstate,
    const partition::ptr_t & partition)
{
    if(ec)
    {
        LOG_WARN("server " << partition->get_server_uuid() << ", partition " \
            << partition->get_uuid() << ": " << ec.message());

        samoa::client::server_response_interface null;
        response_callback(ec, null, partition);
        return;
    }

    if(!request_callback(iface, partition))
    {
        iface.abort_request();
        return;
    }

    build_peer_request(iface, rstate, partition->get_uuid());

    iface.flush_request(
        rstate->get_io_service()->wrap(
            // wrap with request's io-service to synchronize callbacks
            boost::bind(&replication::on_response,
                _1, _2, response_callback, rstate, partition)));
}

void replication::on_response(
    const boost::system::error_code & ec,
    samoa::client::server_response_interface iface,
    const replication::peer_response_callback_t & response_callback,
    const request::state::ptr_t & rstate,
    const partition::ptr_t & partition)
{
    if(ec || iface.get_error_code())
    {
        if(ec)
        {
            LOG_WARN("server " << partition->get_server_uuid() << \
                ", partition " << partition->get_uuid() << \
                ": " << ec.message());
            response_callback(ec, iface, partition);
        }
        else
        {
            LOG_WARN("server " << partition->get_server_uuid() << \
                " remote error on replicated read: " << \
                iface.get_message().error().ShortDebugString());

            if(iface.get_error_code() == 404)
            {
                // cluster-state may be inconsistent
                rstate->get_peer_set()->begin_peer_discovery(
                    partition->get_server_uuid());
            }
            iface.finish_response();

            response_callback(boost::system::errc::make_error_code(
                boost::system::errc::invalid_argument), iface, partition);
        }
        return;
    }

    response_callback(ec, iface, partition);
    iface.finish_response();
}

void replication::build_peer_request(
    samoa::client::server_request_interface & iface,
    const request::state::ptr_t & rstate,
    const core::uuid & part_uuid)
{
    spb::SamoaRequest & samoa_request = iface.get_message();

    samoa_request.set_type(spb::REPLICATE);
    samoa_request.set_key(rstate->get_key());

    // we're handling fanout, the peer doesn't need to
    samoa_request.set_requested_quorum(1);

    samoa_request.mutable_table_uuid()->assign(
        std::begin(rstate->get_table_uuid()),
        std::end(rstate->get_table_uuid()));

    // assign remote partition as primary partition_uuid
    samoa_request.mutable_partition_uuid()->assign(
        std::begin(part_uuid), std::end(part_uuid));

    // assign our local partition as a peer_partition_uuid
    samoa_request.add_peer_partition_uuid()->assign(
        std::begin(rstate->get_primary_partition_uuid()),
        std::end(rstate->get_primary_partition_uuid()));

    // assign _other_ remote partitions as peer_partition_uuid
    for(const core::uuid & other_uuid : rstate->get_peer_partition_uuids())
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

