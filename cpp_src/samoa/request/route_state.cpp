#include "samoa/request/route_state.hpp"
#include "samoa/request/state_exception.hpp"
#include "samoa/server/table.hpp"
#include "samoa/server/local_partition.hpp"
#include "samoa/error.hpp"
#include <sstream>

namespace samoa {
namespace request {

struct partition_order_cmp
{
    bool operator()(const server::partition::ptr_t & lhs, uint64_t rhs) const
    { return lhs->get_ring_position() < rhs; }

    bool operator()(uint64_t lhs, const server::partition::ptr_t & rhs) const
    { return lhs < rhs->get_ring_position(); }
};

route_state::route_state()
 : _ring_position(0),
   _primary_partition_uuid(boost::uuids::nil_uuid()),
   _loaded(false)
{ }

route_state::~route_state()
{ }

void route_state::set_key(std::string && key)
{
    SAMOA_ASSERT(!_loaded);
    _key = std::move(key);
}

void route_state::set_ring_position(uint64_t ring_position)
{
    SAMOA_ASSERT(!_loaded);
    _ring_position = ring_position;
}

void route_state::set_primary_partition_uuid(const core::uuid & uuid)
{
    SAMOA_ASSERT(!_loaded);
    _primary_partition_uuid = uuid;
}

void route_state::add_peer_partition_uuid(const core::uuid & uuid)
{
    SAMOA_ASSERT(!_loaded);

    // insertion sort - a performance assumption here is
    //  that inputs are already mostly sorted
    _peer_partition_uuids.insert(
        std::lower_bound(_peer_partition_uuids.begin(),
            _peer_partition_uuids.end(), uuid),
        uuid);
}

void route_state::load_route_state(const server::table::ptr_t & table)
{
    _loaded = true;

    if(!_ring_position && _key.empty())
    {
        throw state_exception(400, "expected non-empty key");
    }
    else if(!_key.empty())
    {
        _ring_position = table->ring_position(get_key());
    }

    server::table::ring_t::const_iterator it = std::lower_bound(
        table->get_ring().begin(), table->get_ring().end(),
        _ring_position, partition_order_cmp());

    // track whether uuid's are already set (else we'll overwrite below)
    bool has_primary_uuid = has_primary_partition_uuid();
    bool has_peer_uuids = has_peer_partition_uuids();

    for(unsigned count = 0; count != table->get_replication_factor();
        ++count, ++it)
    {
        if(it == table->get_ring().end())
            it = table->get_ring().begin();

        if(has_primary_uuid && \
           (*it)->get_uuid() == get_primary_partition_uuid())
        {
            _primary_partition = \
                std::dynamic_pointer_cast<server::local_partition>(*it);

            if(!_primary_partition)
            {
                std::stringstream err;
                err << "partition " << get_primary_partition_uuid();
                err << " is not local";
                throw state_exception(400, err.str());
            }
            continue;
        }
        else if(!has_primary_uuid && !_primary_partition)
        {
            // pick the first local partition as primary
            _primary_partition = \
                std::dynamic_pointer_cast<server::local_partition>(*it);

            if(_primary_partition)
            {
                _primary_partition_uuid = _primary_partition->get_uuid();
                continue;
            }
        }

        if(has_peer_uuids)
        {
            // this is not a primary partition; verify it's in the
            //   explicit list of peer partition uuids
            auto p_it = std::lower_bound(_peer_partition_uuids.begin(),
                _peer_partition_uuids.end(), (*it)->get_uuid());

            if(p_it == _peer_partition_uuids.end() ||
               *p_it != (*it)->get_uuid())
            {
                std::stringstream err;
                err << "peer-partition uuids is non-empty, but routed peer ";
                err << (*it)->get_uuid() << " is absent";
                throw state_exception(400, err.str()); 
            }
        }
        else
        {
            _peer_partition_uuids.push_back((*it)->get_uuid());
        }

        // this is a secondary partition
        _peer_partitions.push_back(*it);
    }

    if(has_primary_uuid && !get_primary_partition())
    {
        std::stringstream err;
        err << "partition " << get_primary_partition_uuid();
        err << " not found on route of key " << get_key();
        throw state_exception(404, err.str());
    }

    if(has_peer_uuids)
    {
        size_t expected = table->get_replication_factor();

        if(get_primary_partition())
        {
            expected -= 1;
        }

        if(_peer_partition_uuids.size() != expected)
        {
            std::stringstream err;
            err << "expected exactly " << expected;
            err << " or 0 peer-partition uuids";
            throw state_exception(400, err.str()); 
        }
    }
    else
    {
        // sort the range of peer uuids dynamically produced during routing
        std::sort(_peer_partition_uuids.begin(), _peer_partition_uuids.end());
    }
}

void route_state::reset_route_state()
{
    _key.clear();
    _ring_position = 0;
    _primary_partition_uuid = boost::uuids::nil_uuid();
    _peer_partition_uuids.clear();
    _primary_partition.reset();
    _peer_partitions.clear();
    _loaded = false;
}

}
}

