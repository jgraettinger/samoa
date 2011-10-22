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

    /*!
     * Retrieves whether a table-uuid is set.
     *
     * A table-uuid may be explicitly set, or will be dynamically
     *   set after load_table_state()
     */
    bool has_table_uuid() const
    { return !_table_uuid.is_nil(); }

    /*!
     * Retrieves the table-uuid.
     *
     * A table-uuid may be explicitly set, or will be dynamically
     *   set after load_table_state()
     */ 
    const core::uuid & get_table_uuid() const
    { return _table_uuid; }

    /*!
     * Sets the table-uuid of the table.
     *
     * Must be set prior to load_route_state()
     *
     * If both table-uuid and table-name are set,
     *  table-name is ignored.
     */ 
    void set_table_uuid(const core::uuid & uuid);

    /*!
     * Retrieves whether a table-name is set.
     *
     * A table-name may be explicitly set, or will be dynamically
     *   set after load_table_state()
     */
    bool has_table_name() const
    { return !_table_name.empty(); }

    /*!
     * Retrieves the table-name
     *
     * The table-name may be explicitly set, or will be dynamically
     *   set after load_table_state()
     */
    const std::string & get_table_name() const
    { return _table_name; }

    /*!
     * Sets the table-name of the table.
     *
     * Must be set prior to load_route_state()
     *
     * If both table-uuid and table-name are set,
     *  table-name is ignored.
     */ 
    void set_table_name(const std::string &);

    /*!
     * Retrieves the runtime table.
     *
     * Not available until after load_table_state()
     */
    const server::table_ptr_t & get_table() const
    { return _table; }

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

