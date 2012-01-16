
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

    replication::repaired_read(
        boost::bind(&get_blob_handler::on_repaired_read,
            shared_from_this(), _1, rstate),
        rstate);
}

void get_blob_handler::on_repaired_read(
    const boost::system::error_code & ec,
    const request::state::ptr_t & rstate)
{
    if(ec)
    {
        rstate->send_error(504, ec);
        return;
    }

    spb::SamoaResponse & response = rstate->get_samoa_response();

    response.set_replication_success(rstate->get_peer_success_count());
    response.set_replication_failure(rstate->get_peer_failure_count());

    response.mutable_cluster_clock()->CopyFrom(
        rstate->get_local_record().cluster_clock());

    datamodel::blob::value(rstate->get_local_record(),
        [&](const std::string & value)
        { rstate->add_response_data_block(begin(value), end(value)); });

    rstate->flush_response();
}

}
}
}

