
#include "samoa/server/table.hpp"
#include "samoa/server/local_partition.hpp"
#include "samoa/server/remote_partition.hpp"
#include "samoa/server/partition.hpp"
#include "samoa/server/context.hpp"
#include "samoa/server/peer_set.hpp"
#include "samoa/datamodel/blob.hpp"
#include "samoa/core/tasklet_group.hpp"
#include "samoa/core/uuid.hpp"
#include "samoa/error.hpp"
#include "samoa/log.hpp"
#include <boost/smart_ptr/make_shared.hpp>
#include <boost/bind.hpp>
#include <ctime>

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
};

table::table(const spb::ClusterState::Table & ptable,
    const core::uuid & server_uuid,
    const ptr_t & current)
 : _uuid(core::parse_uuid(ptable.uuid())),
   _server_uuid(server_uuid),
   _data_type(datamodel::data_type_from_string(ptable.data_type())),
   _name(ptable.name()),
   _consistency_horizon(ptable.consistency_horizon())
{
    auto it = ptable.partition().begin();
    auto last_it = it;

    std::vector<const spb::ClusterState::Table::Partition *> tmp_ring;
    tmp_ring.reserve(ptable.partition_size());

    for(; it != ptable.partition().end(); ++it)
    {
        // check partition ring-order invariant
        SAMOA_ASSERT(last_it == it || partition_order_cmp()(*last_it, *it));
        last_it = it;

        if(it->dropped())
        {
            // index w/ nullptr, to enforce uuid uniqueness
            SAMOA_ASSERT(_index.insert(std::make_pair(
                core::parse_uuid(it->uuid()), partition::ptr_t())).second);
        }
        else
            tmp_ring.push_back(&(*it));
    }

    // effective factor is bounded by the number of live partitions
    _replication_factor = std::min<unsigned>(
        ptable.replication_factor(), tmp_ring.size());

    for(unsigned i = 0; i != tmp_ring.size(); ++i)
    {
        const spb::ClusterState::Table::Partition & ppart = *tmp_ring[i];

        core::uuid p_uuid = core::parse_uuid(ppart.uuid());
        core::uuid p_server_uuid = core::parse_uuid(ppart.server_uuid());

        partition::ptr_t part, old_part;

        if(current)
        {
            // look for a runtime instance of the partition
            uuid_index_t::const_iterator p_it = current->_index.find(p_uuid);

            if(p_it != current->_index.end())
                old_part = p_it->second;
        }

        // first, map to indices of the partition which bound the current
        //  partition's range of responsible ring positions
        uint64_t range_begin = (i + tmp_ring.size() - 1) % tmp_ring.size();
        uint64_t range_end = (i + _replication_factor - 1) % tmp_ring.size();

        // lookup to map to positions on the ring (inclusive)
        range_begin = tmp_ring[range_begin]->ring_position();
        range_end = tmp_ring[range_end]->ring_position() - 1;

        if(p_server_uuid == _server_uuid)
        {
            part = boost::make_shared<local_partition>(ppart,
                range_begin, range_end,
                boost::dynamic_pointer_cast<local_partition>(old_part));
        }
        else
        {
            part = boost::make_shared<remote_partition>(ppart,
                range_begin, range_end,
                boost::dynamic_pointer_cast<remote_partition>(old_part));
        }

        // index partition on uuid, checking for duplicates
        SAMOA_ASSERT(_index.insert(std::make_pair(p_uuid, part)).second);
        _ring.push_back(part);
    }

    if(_data_type == datamodel::BLOB_TYPE)
    {
        _consistent_merge = boost::bind(&datamodel::blob::consistent_merge,
            _1, _2, _consistency_horizon);
    }
}

table::~table()
{}

const core::uuid & table::get_uuid() const
{ return _uuid; }

datamodel::data_type table::get_data_type() const
{ return _data_type; }

const std::string & table::get_name() const
{ return _name; }

unsigned table::get_replication_factor() const
{ return _replication_factor; }

unsigned table::get_consistency_horizon() const
{ return _consistency_horizon; }

const table::ring_t & table::get_ring() const
{ return _ring; }

partition::ptr_t table::get_partition(const core::uuid & uuid) const
{
    partition::ptr_t result;

    uuid_index_t::const_iterator it = _index.find(uuid);
    if(it != _index.end() && it->second)
    {
        // nullptr is used to mark dropped partitions
        result = it->second;
    }
    return result;
}

const datamodel::merge_func_t & table::get_consistent_merge() const
{ return _consistent_merge; }

uint64_t table::ring_position(const std::string & key) const
{
    return boost::hash<std::string>()(key);
}

void table::spawn_tasklets(const context::ptr_t & context)
{

}

bool table::merge_table(
    const spb::ClusterState::Table & peer,
    spb::ClusterState::Table & local) const
{
    bool dirty = false;

    // is table metadata updated?
    if(local.lamport_ts() < peer.lamport_ts())
    {
        local.set_name(peer.name());
        local.set_replication_factor(peer.replication_factor());
        local.set_consistency_horizon(peer.consistency_horizon());
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
        SAMOA_ASSERT(last_p_it == p_it ||
            partition_order_cmp()(*last_p_it, *p_it));
        last_p_it = p_it;

        // eventually, we'll want to study local partitions & the
        //  ring in general, and determine the partition subset we
        //  actually need to keep around. for now, keep everything

        if(l_it == local_parts.end() ||
           partition_order_cmp()(*p_it, *l_it))
        {
            // we don't know about this partition

            // index where the partition should appear
            int local_ind = std::distance(local_parts.begin(), l_it);

            // build new partition protobuf record
            spb::ClusterState::Table::Partition * new_part = local_parts.Add();

            new_part->set_uuid(p_it->uuid());
            new_part->set_ring_position(p_it->ring_position());

            if(p_it->dropped())
            {
                LOG_INFO("discovered (dropped) partition " << p_it->uuid());
                new_part->set_dropped(true);
                new_part->set_dropped_timestamp(time(0));
            }
            else
            {
                // check that peer isn't trying to tell us of our own partition
                core::uuid server_uuid = core::parse_uuid(p_it->server_uuid());
                SAMOA_ASSERT(server_uuid != _server_uuid);

                LOG_INFO("discovered partition " << p_it->uuid());

                new_part->set_server_uuid(p_it->server_uuid());

                // build a temporary remote_partition instance to build
                //  our local view of the partition
                remote_partition(*new_part, 0, 0,
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
                l_it->set_dropped_timestamp(time(0));
                dirty = true;
            }
            else
            {
                // local & peer partitions are both live; pass down to
                //   partition instance to continue merging
                uuid_index_t::const_iterator uuid_it = \
                    _index.find(core::parse_uuid(l_it->uuid()));

                SAMOA_ASSERT(uuid_it != _index.end());

                dirty = uuid_it->second->merge_partition(*p_it, *l_it) || dirty;
            }
            ++l_it; ++p_it;
        }
    }
    return dirty;
}

}
}

