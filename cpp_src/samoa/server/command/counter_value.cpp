
#include "samoa/server/command/counter_value.hpp"
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
#include "samoa/datamodel/counter.hpp"
#include <boost/bind.hpp>

namespace samoa {
namespace server {
namespace command {

namespace spb = samoa::core::protobuf;
using std::begin;
using std::end;

void counter_value_handler::handle(const request::state::ptr_t & rstate)
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
        boost::bind(&counter_value_handler::on_repaired_read,
            shared_from_this(), _1, rstate),
        rstate);
}

void counter_value_handler::on_repaired_read(
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

    response.set_counter_value(
        datamodel::counter::value(rstate->get_local_record()));

    rstate->flush_response();
}

}
}
}

