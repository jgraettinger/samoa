#include "samoa/request/request_state.hpp"
#include "samoa/state/state_exception.hpp"

namespace samoa {
namespace request {

namespace spb = samoa::core::protobuf;

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

void state::parse_from_protobuf_request()
{
    const spb::SamoaRequest & request = get_samoa_request();

    client_state::get_samoa_response().set_type(request.type());

    if(request.has_table_uuid())
    {
        core::uuid tmp;
        if(!core::parse_uuid(request.table_uuid(), tmp))
        {
            std::stringstream err;
            err << "malformed table-uuid " << request.table_uuid();
            throw state_exception(400, err.str());
        }

        get_table_state().set_table_uuid(tmp);
    }

    if(request.has_table_name())
    {
        get_table_state().set_table_name(request.table_name());
    }

    if(request.has_key())
    {
        get_route_state().set_key(request.key());
    }

    if(request.has_partition_uuid())
    {
        core::uuid tmp;
        if(!core::parse_uuid(request.partition_uuid(), tmp))
        {
            std::stringstream err;
            err << "malformed partition-uuid " << request.partition_uuid();
            throw state_exception(400, err.str());
        }

        get_route_state().set_primary_partition_uuid(tmp);
    }

    for(auto it = request.peer_partition_uuid().begin();
        it != request.peer_partition_uuid().end(); ++it)
    {
        core::uuid tmp;
        if(!core::parse_uuid(*it, tmp))
        {
            std::stringstream err;
            err << "malformed peer-partition-uuid " << *it;
            throw state_exception(400, err.str());
        }

        get_route_state().add_peer_partition_uuid(tmp);
    }

    get_replication_state().set_quorum_count(request.requested_quorum());
}

}
}

