#include "samoa/request/request_state.hpp"
#include "samoa/request/state_exception.hpp"
#include "samoa/server/table.hpp"

namespace samoa {
namespace request {

namespace spb = samoa::core::protobuf;

state::state()
{ }

state::~state()
{ }

void state::load_table_state()
{
    const spb::SamoaRequest & request = get_samoa_request();

    if(request.has_table_uuid())
    {
        core::uuid tmp = core::try_parse_uuid(request.table_uuid());
        if(tmp.is_nil())
        {
            std::stringstream err;
            err << "malformed table-uuid " << request.table_uuid();
            throw state_exception(400, err.str());
        }

        table_state::set_table_uuid(tmp);
    }

    if(request.has_table_name())
    {
        table_state::set_table_name(request.table_name());
    }

    table_state::load_table_state(get_table_set());
}

void state::load_route_state()
{
    SAMOA_ASSERT(table_state::get_table());

    const spb::SamoaRequest & request = get_samoa_request();

    if(request.has_key())
    {
        route_state::set_key(std::string(request.key()));
    }

    if(request.has_partition_uuid())
    {
        core::uuid tmp = core::try_parse_uuid(request.partition_uuid());
        if(tmp.is_nil())
        {
            std::stringstream err;
            err << "malformed partition-uuid " << request.partition_uuid();
            throw state_exception(400, err.str());
        }

        route_state::set_primary_partition_uuid(tmp);
    }

    for(auto it = request.peer_partition_uuid().begin();
        it != request.peer_partition_uuid().end(); ++it)
    {
        core::uuid tmp = core::try_parse_uuid(*it);
        if(tmp.is_nil())
        {
            std::stringstream err;
            err << "malformed peer-partition-uuid " << *it;
            throw state_exception(400, err.str());
        }

        route_state::add_peer_partition_uuid(tmp);
    }

    route_state::load_route_state(get_table());
}

void state::load_replication_state()
{
    SAMOA_ASSERT(route_state::get_primary_partition());
    SAMOA_ASSERT(route_state::get_peer_partitions().size() + 1 == \
        get_table()->get_replication_factor());

    const spb::SamoaRequest & request = get_samoa_request();

    replication_state::set_quorum_count(request.requested_quorum());

    replication_state::load_replication_state(
        get_table()->get_replication_factor());
}

client_state & state::initialize_from_client(
    const server::client_ptr_t & client)
{
    load_io_service_state(client->get_io_service());
    load_context_state(client->get_context());
    load_client_state(client);

    return *this;
}

void state::reset_state()
{
    reset_io_service_state();
    reset_context_state();
    reset_client_state();
    reset_table_state();
    reset_route_state();
    reset_record_state();
    reset_replication_state();
}

}
}

