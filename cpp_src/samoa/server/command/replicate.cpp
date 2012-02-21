
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

        auto on_write = [rstate](
            const boost::system::error_code & ec,
            const datamodel::merge_result &)
        {
            if(ec)
            {
                LOG_WARN(ec.message());
                rstate->send_error(500, ec);
                return;
            }
            rstate->flush_response();
        };

        rstate->get_primary_partition()->write(
            on_write,
            rstate->get_table()->get_consistent_merge(),
            rstate,
            false);
    }
    else
    {
        auto on_read = [rstate](
            const boost::system::error_code & ec, bool found)
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
            rstate->flush_response();
        };

        rstate->get_primary_partition()->read(on_read, rstate);
    }
}

}
}
}

