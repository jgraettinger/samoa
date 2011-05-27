#include "samoa/server/table_set.hpp"
#include "samoa/server/table.hpp"
#include "samoa/error.hpp"
#include "samoa/log.hpp"
#include <boost/smart_ptr/make_shared.hpp>

namespace samoa {
namespace server {

table_set::table_set(const spb::ClusterState & state,
    const ptr_t & current)
{
    auto it = state.table().begin();
    auto last_it = it;

    core::uuid local_uuid = core::uuid_from_hex(state.local_uuid());

    for(; it != state.table().end(); ++it)
    {
        // assert table uuid order invariant
        SAMOA_ASSERT(it == last_it || last_it->uuid() < it->uuid());
        last_it = it;

        if(it->dropped())
        {
            continue;
        }

        core::uuid uuid = core::uuid_from_hex(it->uuid());

        table::ptr_t tbl, old_tbl;

        if(current)
        {
            uuid_index_t::const_iterator t_it = \
                current->_uuid_index.find(uuid);

            if(t_it != current->_uuid_index.end())
                old_tbl = t_it->second;
        }

        tbl = boost::make_shared<table>(*it, local_uuid, old_tbl);

        // index table on uuid
        SAMOA_ASSERT(_uuid_index.insert(std::make_pair(uuid, tbl)).second);

        if(!_name_index.insert(std::make_pair(tbl->get_name(), tbl)).second)
        {
            // mark duplicate names with nullptr
            _name_index[tbl->get_name()] = table::ptr_t();
        }
    }
}

table::ptr_t table_set::get_table_by_uuid(const core::uuid & uuid)
{
    uuid_index_t::const_iterator it = _uuid_index.find(uuid);
    if(it == _uuid_index.end())
    {
        error::throw_not_found("table uuid", core::to_hex(uuid));
    }
    return it->second;
}

table::ptr_t table_set::get_table_by_name(const std::string & name)
{
    name_index_t::const_iterator it = _name_index.find(name);

    if(it == _name_index.end())
    {
        error::throw_not_found("table name", name);
    }
    if(!it->second)
    {
        // nullptr means multiple tables have this name
        error::throw_not_found("table name (ambiguous)", name);
    }
    return it->second;
}

bool table_set::merge_table_set(const spb::ClusterState & peer,
    spb::ClusterState & local) const
{
    bool dirty = false;

    core::uuid local_uuid = core::uuid_from_hex(local.local_uuid());

    typedef google::protobuf::RepeatedPtrField<
        spb::ClusterState::Table> p_tables_t;

    const p_tables_t & peer_tables = peer.table();
    p_tables_t & local_tables = *local.mutable_table();

    p_tables_t::const_iterator p_it = peer_tables.begin();
    p_tables_t::iterator l_it = local_tables.begin();

    p_tables_t::const_iterator last_p_it = p_it;

    while(p_it != peer_tables.end())
    {
        // check peer table order invariant
        SAMOA_ASSERT(last_p_it == p_it || last_p_it->uuid() < p_it->uuid());
        last_p_it = p_it; 

        if(l_it == local_tables.end() || p_it->uuid() < l_it->uuid())
        {
            // we don't know about this table

            // index where the table should appear
            int local_ind = std::distance(local_tables.begin(), l_it);

            // build new table protobuf record
            spb::ClusterState::Table * new_ptable = local_tables.Add();
            new_ptable->set_uuid(p_it->uuid());

            if(p_it->dropped())
            {
                LOG_INFO("discovered (dropped) table " << p_it->uuid());
                new_ptable->set_dropped(true);
            }
            else
            {
                LOG_INFO("discovered table " << p_it->uuid());

                // set ctor-required fields of the new table
                new_ptable->set_data_type(p_it->data_type());

                // build a temporary table instance to build
                //  our local view of the table
                table(*new_ptable, local_uuid, table::ptr_t()
                    ).merge_table(*p_it, *new_ptable);
            }

            // bubble new_ptable up to appear at local_ind,
            //  re-establishing sorted order on uuid
            for(int j = local_tables.size(); --j != local_ind;)
            {
                local_tables.SwapElements(j, j - 1);
            }

            // l_it may have been invalidated by local_tabels.Add()
            l_it = local_tables.begin() + local_ind;

            dirty = true;
            ++l_it; ++p_it;
        }
        else if(p_it->uuid() > l_it->uuid())
        {
            //  peer doesn't know of this table => ignore
            ++l_it;
        }
        else
        {
            // p_it->uuid() == l_it->uuid();
            //  these refer to the same table

            if(l_it->dropped())
            {
                // ignore changes to locally-dropped tables
            }
            else if(p_it->dropped())
            {
                // locally-live table is remotely dropped
                LOG_INFO("discovered table " + p_it->uuid() + " was dropped");
                l_it->Clear();
                l_it->set_uuid(p_it->uuid());
                l_it->set_dropped(true);
                dirty = true;
            }
            else
            {
                // local & peer tables are both live;  pass down to
                //   table instance to continue merging
                uuid_index_t::const_iterator uuid_it = \
                    _uuid_index.find(core::uuid_from_hex(l_it->uuid()));

                SAMOA_ASSERT(uuid_it != _uuid_index.end());

                dirty = uuid_it->second->merge_table(*p_it, *l_it) || dirty;
            }
            ++l_it; ++p_it;
        }
    }
    return dirty;
}

}
}

