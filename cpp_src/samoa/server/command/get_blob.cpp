
#include "samoa/server/command/get_blob.hpp"
#include "samoa/server/client.hpp"
#include "samoa/server/table.hpp"
#include "samoa/server/peer_set.hpp"
#include "samoa/server/partition.hpp"
#include "samoa/server/local_partition.hpp"
#include "samoa/server/replication.hpp"
#include "samoa/persistence/persister.hpp"
#include "samoa/request/request_state.hpp"
#include "samoa/request/state_exception.hpp"
#include "samoa/core/protobuf/fwd.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include "samoa/datamodel/blob.hpp"
#include <boost/bind.hpp>

namespace samoa {
namespace server {
namespace command {

namespace spb = samoa::core::protobuf;
using std::begin;
using std::end;

void get_blob_handler::handle(const request::state::ptr_t & rstate)
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

    auto on_read = [rstate](const boost::system::error_code & ec, bool)
    {
        if(ec)
        {
            rstate->send_error(504, ec);
            return;
        }

        rstate->get_samoa_response().mutable_cluster_clock()->CopyFrom(
            rstate->get_local_record().cluster_clock());

        datamodel::blob::value(rstate->get_local_record(),
            [&](const std::string & value)
            { rstate->add_response_data_block(begin(value), end(value)); });

        rstate->flush_response();
    };

    rstate->get_primary_partition()->read(on_read, rstate);
}

}
}
}

