
#include "samoa/server/table.hpp"
#include "samoa/server/local_partition.hpp"
#include "samoa/server/remote_partition.hpp"
#include "samoa/server/partition.hpp"
#include "samoa/persistence/data_type.hpp"
#include "samoa/error.hpp"
#include "samoa/log.hpp"
#include <boost/smart_ptr/make_shared.hpp>

namespace samoa {
namespace server {

struct partition_order_cmp
{
    bool operator()(const spb::ClusterState::Table::Partition & lhs,
                    const spb::ClusterState::Table::Partition & rhs)
    {
        if(lhs.ring_position() < rhs.ring_position())
            return true;

        return lhs.ring_position() == rhs.ring_position() && \
            lhs.uuid() < rhs.uuid();
    }

    bool operator()(const partition::ptr_t & lhs, unsigned rhs) const
    { return lhs->get_ring_position() < rhs; }

    bool operator()(unsigned lhs, const partition::ptr_t & rhs) const
    { return lhs < rhs->get_ring_position(); }
};

table::table(const spb::ClusterState::Table & ptable,
    const core::uuid & server_uuid,
    const ptr_t & current)
 : _uuid(core::uuid_from_hex(ptable.uuid())),
   _server_uuid(server_uuid),
   _data_type(persistence::data_type_from_string(ptable.data_type())),
   _name(ptable.name()),
   _repl_factor(ptable.replication_factor())
{
    auto it = ptable.partition().begin();
    auto last_it = it;

    for(; it != ptable.partition().end(); ++it)
    {
        // check partitions ring-order invariant
        SAMOA_ASSERT(partition_order_cmp()(*it, *last_it));
        last_it = it;

        if(it->dropped())
            continue;

        core::uuid p_uuid = core::uuid_from_hex(it->uuid());
        core::uuid p_server_uuid = core::uuid_from_hex(it->server_uuid());

        partition::ptr_t old_part = (current ?
            current->get_partition(p_uuid) : partition::ptr_t());

        partition::ptr_t part;

        if(p_server_uuid == _server_uuid)
        {
            part = boost::make_shared<local_partition>(*it,
                boost::dynamic_pointer_cast<local_partition>(old_part));
        }
        else
        {
            part = boost::make_shared<remote_partition>(*it,
                boost::dynamic_pointer_cast<remote_partition>(old_part));
        }

        // index partition on uuid, checking for duplicates
        SAMOA_ASSERT(!_index.insert(std::make_pair(p_uuid, part)).second);
        _ring.push_back(part);
    }
}

table::~table()
{}

const core::uuid & table::get_uuid() const
{ return _uuid; }

persistence::data_type table::get_data_type() const
{ return _data_type; }

const std::string & table::get_name() const
{ return _name; }

size_t table::get_replication_factor() const
{ return _repl_factor; }

const table::ring_t & table::get_ring() const
{ return _ring; }

partition::ptr_t table::get_partition(const core::uuid & uuid) const
{
    uuid_index_t::const_iterator it = _index.find(uuid);
    if(it == _index.end())
    {
        error::throw_not_found("partition", core::to_hex(uuid));
    }
    return it->second;
}

void table::route_key(const std::string & key,
    table::ring_t & out) const
{
    unsigned key_pos = boost::hash<std::string>()(key);

    ring_t::const_iterator it = std::lower_bound(
        _ring.begin(), _ring.end(), key_pos, partition_order_cmp());

    out.clear();
    while(out.size() != _repl_factor && out.size() != _ring.size())
    {
        out.push_back(*it);

        if(++it == _ring.end())
            it = _ring.begin();
    }
}

bool table::merge_table(
    const spb::ClusterState::Table & peer,
    spb::ClusterState::Table & local)
{
    bool dirty = false;

    // is table metadata updated?
    if(local.lamport_ts() < peer.lamport_ts())
    {
        local.set_name(peer.name());
        local.set_replication_factor(peer.replication_factor());
        local.set_lamport_ts(peer.lamport_ts());
        dirty = true;
    }

    typedef google::protobuf::RepeatedPtrField<
        spb::ClusterState::Table::Partition> p_parts_t;

    const p_parts_t & peer_parts = peer.partition();
    p_parts_t & local_parts = *local.mutable_partition();

    p_parts_t::const_iterator p_it = peer_parts.begin();
    p_parts_t::iterator l_it = local_parts.begin();

    p_parts_t::const_iterator last_p_it = p_it;

    while(p_it != peer_parts.end())
    {
        // check partition order invariant
        SAMOA_ASSERT(partition_order_cmp()(*p_it, *last_p_it));
        last_p_it = p_it;

        // eventually, we'll want to study local partitions & the
        //  ring in general, and determine the partition subset we
        //  actually need to keep around. for now, keep everything

        if(l_it == local_parts.end() ||
           partition_order_cmp()(*p_it, *l_it))
        {
            // we don't know about this partition
            LOG_INFO("discovered partition " << p_it->uuid());

            core::uuid server_uuid = core::uuid_from_hex(p_it->server_uuid());

            // a peer cannot tell us of our own partition
            SAMOA_ASSERT(server_uuid == _server_uuid);

            // index where the partition should appear
            int local_ind = std::distance(local_parts.begin(), l_it);

            // build new partition protobuf record
            spb::ClusterState::Table::Partition * new_part = local_parts.Add();

            new_part->set_uuid(p_it->uuid());
            new_part->set_ring_position(p_it->ring_position());
            new_part->set_server_uuid(p_it->server_uuid());

            if(p_it->dropped())
            {
                LOG_INFO("partition " << p_it->uuid() << " was dropped");
                new_part->set_dropped(true);
            }
            else
            {
                // build a temporary remote_partition instance to build
                //  our local view of the partition
                remote_partition(*new_part,
                        remote_partition::ptr_t()
                    ).merge_partition(*p_it, *new_part);
            }

            // bubble new_part to appear at local_ind,
            //  re-establishing sorted order on (ring-position, uuid)
            for(int j = local_parts.size(); --j != local_ind;)
            {
                local_parts.SwapElements(j, j - 1);
            }

            // l_it may have been invalidated by local.Add()
            l_it = local_parts.begin() + local_ind;

            dirty = true;
            ++l_it; ++p_it;
        }
        else if(partition_order_cmp()(*l_it, *p_it))
        {
            // peer doesn't know of this partition => ignore
            ++l_it;
        }
        else
        {
            // l_it & p_it reference the same partition

            if(l_it->dropped())
            {
                // ignore changes to locally-dropped partitions
            }
            else if(p_it->dropped())
            {
                // locally-live partition is remotely dropped
                LOG_INFO("partition " << p_it->uuid() << " was dropped");
                l_it->Clear();
                l_it->set_uuid(p_it->uuid());
                l_it->set_ring_position(p_it->ring_position());
                l_it->set_dropped(true);
                dirty = true;
            }
            else
            {
                // local & peer partitions are both live; pass down to
                //   partition instance to continue merging
                uuid_index_t::const_iterator uuid_it = \
                    _index.find(core::uuid_from_hex(l_it->uuid()));

                assert(uuid_it != _index.end());

                dirty = dirty || uuid_it->second->merge_partition(*p_it, *l_it);
            }
            ++l_it; ++p_it;
        }
    }
    return dirty;
}

}
}

