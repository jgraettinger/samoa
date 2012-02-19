
#include "samoa/server/command/replicate.hpp"
#include "samoa/server/client.hpp"
#include "samoa/server/local_partition.hpp"
#include "samoa/server/table.hpp"
#include "samoa/server/replication.hpp"
#include "samoa/server/peer_set.hpp"
#include "samoa/server/digest.hpp"
#include "samoa/request/request_state.hpp"
#include "samoa/request/state_exception.hpp"
#include "samoa/persistence/persister.hpp"
#include "samoa/core/protobuf/fwd.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include "samoa/core/protobuf/zero_copy_input_adapter.hpp"
#include "samoa/core/protobuf/zero_copy_output_adapter.hpp"
#include "samoa/core/fwd.hpp"
#include "samoa/error.hpp"
#include "samoa/log.hpp"
#include <boost/lexical_cast.hpp>
#include <boost/bind.hpp>

namespace samoa {
namespace server {
namespace command {

namespace spb = samoa::core::protobuf;

void replicate_handler::handle(const request::state::ptr_t & rstate)
{
    rstate->load_table_state();
    rstate->load_route_state();

    if(!rstate->get_primary_partition())
    {
        // no primary partition; forward to a better peer
        rstate->get_peer_set()->forward_request(rstate);
        return;
    }

    rstate->load_replication_state();

    bool write_request = !rstate->get_request_data_blocks().empty();

    if(write_request)
    {
        if(rstate->get_request_data_blocks().size() != 1)
        {
            rstate->send_error(400, "expected just one data block");
            return;
        }

        // parse into remote-record
        core::protobuf::zero_copy_input_adapter zci_adapter(
            rstate->get_request_data_blocks()[0]);
        SAMOA_ASSERT(rstate->get_remote_record(
            ).ParseFromZeroCopyStream(&zci_adapter));

        rstate->get_primary_partition()->get_persister()->put(
            boost::bind(&replicate_handler::on_write,
                shared_from_this(), _1, _2, _3, rstate),
            datamodel::merge_func_t(
                rstate->get_table()->get_consistent_merge()),
            rstate->get_key(),
            rstate->get_remote_record(),
            rstate->get_local_record());
    }
    else
    {
        // kick off a (possibly replicated) read
        replication::repaired_read(
            boost::bind(&replicate_handler::on_read,
                shared_from_this(), _1, _2, rstate),
            rstate);
    }
}

void replicate_handler::on_write(
    const boost::system::error_code & ec,
    const datamodel::merge_result & merge_result,
    const core::murmur_checksum_t & checksum,
    const request::state::ptr_t & rstate)
{
    if(ec)
    {
        LOG_WARN(ec.message());
        rstate->send_error(500, ec);
        return;
    }

    if(rstate->get_quorum_count() != 1 || merge_result.remote_is_stale)
    {
    	// force replication fanout
        replication::replicated_write(rstate, checksum);
    }
    else
    {
        // assume that all peers have also been replicated to as well
        for(const partition::ptr_t & partition : rstate->get_peer_partitions())
        {
            partition->get_digest()->add(checksum);
        }
        rstate->flush_response();
    }
}

void replicate_handler::on_read(const boost::system::error_code & ec,
    bool found, const request::state::ptr_t & rstate)
{
    if(ec)
    {
        LOG_WARN(ec.message());
        rstate->send_error(500, ec);
        return;
    }

    if(found)
    {
        core::protobuf::zero_copy_output_adapter zco_adapter;
        SAMOA_ASSERT(rstate->get_local_record(
            ).SerializeToZeroCopyStream(&zco_adapter));

        rstate->add_response_data_block(zco_adapter.output_regions());
    }

    on_client_response(rstate);
}

void replicate_handler::on_client_response(
    const request::state::ptr_t & rstate)
{
    rstate->get_samoa_response().set_replication_success(
        rstate->get_peer_success_count());
    rstate->get_samoa_response().set_replication_failure(
        rstate->get_peer_failure_count());

    rstate->flush_response();
}

}
}
}

