#include "samoa/request/request_state.hpp"
#include "samoa/request/state_exception.hpp"
#include "samoa/datamodel/clock_util.hpp"
#include "samoa/server/table.hpp"

namespace samoa {
namespace request {

namespace spb = samoa::core::protobuf;

state::state()
{ }

state::~state()
{ }

void state::flush_response()
{
    client_state::flush_response(shared_from_this());
}

void state::send_error(unsigned err_code, const std::string & err_msg)
{
    client_state::send_error(shared_from_this(), err_code, err_msg);
}

void state::send_error(unsigned err_code,
    const boost::system::error_code & err_msg)
{
    client_state::send_error(shared_from_this(), err_code, err_msg);
}

client_state & state::mutable_client_state()
{
    SAMOA_ASSERT(!get_client());
    return *this;
}

void state::load_table_state()
{
    table_state::load_table_state(get_table_set());
}

void state::load_route_state()
{
    SAMOA_ASSERT(table_state::get_table());
    route_state::load_route_state(get_table());
}

void state::load_replication_state()
{
    SAMOA_ASSERT(route_state::get_primary_partition());
    SAMOA_ASSERT(route_state::get_peer_partitions().size() + 1 == \
        get_table()->get_replication_factor());

    replication_state::load_replication_state(
        get_table()->get_replication_factor());
}

void state::initialize_from_client(server::client_ptr_t client)
{
    load_io_service_state(client->get_io_service());
    load_context_state(client->get_context());
    load_client_state(std::move(client));
}

void state::parse_samoa_request()
{
    const spb::SamoaRequest & request = client_state::get_samoa_request();

    // table_state
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

    // route_state
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

    // replication_state
    replication_state::set_quorum_count(request.requested_quorum());

    // cluster-clock
    if(request.has_cluster_clock() &&
       !datamodel::clock_util::validate(request.cluster_clock()))
    {
        std::stringstream err;
        err << "malformed cluster-clock";
        throw state_exception(400, err.str());
    }
}

void state::reset_state()
{
    reset_context_state();
    reset_client_state();
    reset_table_state();
    reset_route_state();
    reset_record_state();
    reset_replication_state();
}

}
}

