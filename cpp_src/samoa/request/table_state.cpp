#include "samoa/request/table_state.hpp"
#include "samoa/request/state_exception.hpp"
#include "samoa/server/table_set.hpp"
#include "samoa/server/table.hpp"
#include <sstream>

namespace samoa {
namespace request {

table_state::table_state()
 : _table_uuid(boost::uuids::nil_uuid())
{ }

table_state::~table_state()
{ }

void table_state::set_table_uuid(const core::uuid & uuid)
{
    _table_uuid = uuid;
}

void table_state::set_table_name(const std::string & name)
{
    if(name.empty())
    {
        throw state_exception(400, "expected a non-empty table name");
    }

    _table_name = name;
}

void table_state::load_table_state(const server::table_set::ptr_t & table_set)
{
    if(has_table_uuid())
    {
        _table = table_set->get_table(_table_uuid);

        if(!_table)
        {
            std::stringstream err;
            err << "table-uuid " << _table_uuid;
            throw state_exception(404, err.str());
        }

        _table_name = _table->get_name();
    }
    else if(has_table_name())
    {
        _table = table_set->get_table_by_name(_table_name);

        if(!_table)
        {
            std::stringstream err;
            err << "table-name " << _table_name;
            throw state_exception(404, err.str());
        }

        _table_uuid = _table->get_uuid();
    }
    else
    {
        throw state_exception(404, "expected table-uuid or table-name");
    }
}

void table_state::reset_table_state()
{
    _table_uuid = boost::uuids::nil_uuid();
    _table_name.clear();
    _table.reset();
}

}
}

