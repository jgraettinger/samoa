
#include "samoa/server/command/basic_replicate.hpp"
#include "samoa/server/client.hpp"
#include "samoa/server/local_partition.hpp"
#include "samoa/server/table.hpp"
#include "samoa/server/replication.hpp"
#include "samoa/server/request_state.hpp"
#include "samoa/persistence/persister.hpp"
#include "samoa/core/protobuf/fwd.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include "samoa/error.hpp"
#include "samoa/log.hpp"
#include <boost/lexical_cast.hpp>
#include <boost/bind.hpp>

namespace samoa {
namespace server {
namespace command {

namespace spb = samoa::core::protobuf;

void replicate_handler::handle(const request_state::ptr_t & rstate)
{
    bool write_request = !rstate->get_request_data_blocks().empty();

    if(write_request)
    {
        if(rstate->get_request_data_blocks().size() != 1)
        {
            rstate->send_client_error(400, "expected just one data block");
            return;
        }

        // parse into remote-record
        core::zero_copy_input_adapter zci_adapter(
            rstate->get_request_data_blocks()[0]);
        SAMOA_ASSERT(rstate->get_remote_record(
            ).ParseFromZeroCopyStream(&zci_adapter));

        rstate->get_primary_partition()->get_persister()->put(
            boost::bind(&replicate_handler::on_write,
                shared_from_this(), _1, _2, rstate),
            datamodel::merge_func_t(
                rstate->get_table()->get_consistent_merge()),
            rstate->get_key(),
            rstate->get_remote_record(),
            rstate->get_local_record());
    }
    else
    {
        rstate->get_primary_partition()->get_persister()->get(
            boost::bind(&replicate_handler::on_read,
                shared_from_this(), _1, _2, rstate),
            rstate->get_key(),
            rstate->get_local_record());
    }
}

void replicate_handler::on_write(const boost::system::error_code & ec,
    const datamodel::merge_result & merge_result,
    const request_state::ptr_t & rstate)
{
    if(ec)
    {
        LOG_WARN(ec.message());
        rstate->send_client_error(500, ec);
        return;
    }

    rstate->finish_client_response();

    if(merge_result.remote_is_stale)
    {
        // start a reverse replication, to synchronize remote peer
        replication::replicated_write(
            boost::bind(&replicate_handler::on_reverse_replication,
                shared_from_this(), _1),
            rstate);
    }
}

void replicate_handler::on_read(const boost::system::error_code & ec,
    bool found, const request_state::ptr_t & rstate)
{
    if(ec)
    {
        LOG_WARN(ec.message());
        rstate->send_client_error(500, ec);
        return;
    }

    if(found)
    {
        core::zero_copy_output_adapter zco_adapter;
        rstate->get_local_record().SerializeToZeroCopyStream(&zco_adapter);
        rstate->get_samoa_response().add_data_block_length(
            zco_adapter.ByteCount());

        rstate->start_client_response();
        rstate->get_client()->write_interface().queue_write(
            zco_adapter.output_regions());
    }

    rstate->finish_client_response();
}

void replicate_handler::on_reverse_replication(
    const boost::system::error_code & ec)
{
    if(ec)
    {
        LOG_WARN(ec.message());
    }
}

}
}
}

