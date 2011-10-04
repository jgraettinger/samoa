#include "samoa/server/state/route_state.hpp"
#include "samoa/server/table.hpp"
#include "samoa/server/local_partition.hpp"
#include "samoa/server/partition_peers.hpp"
#include <sstream>

namespace samoa {
namespace server {
namespace state {

route_state::route_state()
 : _primary_partition_uuid(boost::uuids::nil_uuid()),
   _ring_position(0)
{ }

void route_state::load_route_state(const table::ptr_t & table,
    const peer_set::ptr_t & peer_set)
{
    if(has_key())
    {
        _ring_position = table->ring_position(get_key());
    }

    if(has_primary_partition_uuid())
    {
        _primary_partition = \
            boost::dynamic_pointer_cast<local_partition>(
                table->get_partition(_primary_partition_uuid));

        if(!_primary_partition)
        {
            std::stringstream err;
            err << "local partition-uuid " << _primary_partition_uuid;
            throw state_exception(404, err.str());
        }
    }


}

}
}
}

