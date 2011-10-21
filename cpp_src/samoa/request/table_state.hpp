#ifndef SAMOA_REQUEST_TABLE_STATE_HPP
#define SAMOA_REQUEST_TABLE_STATE_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/core/uuid.hpp"
#include <string>

namespace samoa {
namespace request {

class table_state
{
public:

    table_state();

    virtual ~table_state();

    bool has_table_uuid() const
    { return !_table_uuid.is_nil(); }

    const core::uuid & get_table_uuid() const
    { return _table_uuid; }

    bool has_table_name() const
    { return !_table_name.empty(); }

    const std::string & get_table_name() const
    { return _table_name; }

    const server::table_ptr_t & get_table() const
    { return _table; }

    void set_table_uuid(const core::uuid & uuid);

    void set_table_name(const std::string &);

    void load_table_state(const server::table_set_ptr_t &);

    void reset_table_state();

private:

    core::uuid _table_uuid;
    std::string _table_name;

    server::table_ptr_t _table;
};

}
}

#endif

