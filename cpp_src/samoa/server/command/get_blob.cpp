
#include "samoa/server/command/get_blob.hpp"
#include "samoa/server/client.hpp"
#include "samoa/server/table.hpp"
#include "samoa/server/peer_set.hpp"
#include "samoa/server/partition.hpp"
#include "samoa/server/local_partition.hpp"
#include "samoa/server/request_state.hpp"
#include "samoa/server/replication.hpp"
#include "samoa/persistence/persister.hpp"
#include "samoa/core/protobuf/fwd.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include "samoa/datamodel/blob.hpp"
#include <boost/bind.hpp>

namespace samoa {
namespace server {
namespace command {

namespace spb = samoa::core::protobuf;

void get_blob_handler::handle(const client::ptr_t & client)
{
    request_state::ptr_t rstate = request_state::extract(client);

    if(!rstate->get_primary_partition())
    {
        rstate->get_peer_set()->forward_request(rstate);
        return;
    }

    if(rstate->get_quorum_count() == 1)
    {
        // simple case: start a persister read into local-record
        rstate->get_primary_partition()->get_persister()->get(
            boost::bind(&get_blob_handler::on_replicated_read,
                shared_from_this(), _1, rstate),
            rstate->get_key(),
            rstate->get_local_record());
    }
    else
    {
        // quorom read: spawn replication-read requests to peers
        replication::replicated_read(
            boost::bind(&get_blob_handler::on_replicated_read,
                shared_from_this(), _1, rstate),
            rstate);
    }
}

void get_blob_handler::on_replicated_read(
    const boost::system::error_code & ec,
    const request_state::ptr_t & rstate)
{
    if(ec)
    {
        rstate->send_client_error(504, ec);
        return;
    }

    if(rstate->get_remote_record().has_cluster_clock() ||
       rstate->get_remote_record().blob_value_size())
    {
        // speculatively write the merged remote record; as a side-effect,
        //  local-record will be populated with the merged result
        rstate->get_primary_partition()->get_persister()->put(
            boost::bind(&get_blob_handler::on_retrieve,
                shared_from_this(), _1, rstate),
            datamodel::merge_func_t(
                rstate->get_table()->get_consistent_merge()),
            rstate->get_key(),
            rstate->get_remote_record(),
            rstate->get_local_record());
    }
    else
    {
        // no peer records exist; fall back to a persister read
        rstate->get_primary_partition()->get_persister()->get(
            boost::bind(&get_blob_handler::on_retrieve,
                shared_from_this(), _1, rstate),
            rstate->get_key(),
            rstate->get_local_record());
    }
}

void get_blob_handler::on_retrieve(
    const boost::system::error_code & ec,
    const request_state::ptr_t & rstate)
{
    if(ec)
    {
        rstate->send_client_error(504, ec);
        return;
    }

    datamodel::blob::send_blob_value(rstate->get_client(),
        rstate->get_local_record());
}

}
}
}

