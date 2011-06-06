
#ifndef SAMOA_SERVER_TABLE_SET_HPP
#define SAMOA_SERVER_TABLE_SET_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include "samoa/core/uuid.hpp"
#include <boost/unordered_map.hpp>

namespace samoa {
namespace server {

namespace spb = samoa::core::protobuf;

class table_set
{
public:

    typedef table_set_ptr_t ptr_t;

    table_set(const spb::ClusterState &,
        const ptr_t &);

    table_ptr_t get_table(const core::uuid &);

    table_ptr_t get_table_by_name(const std::string &);

    //! Merges peer table descriptions into the local description
    /*!
        \return true iff local was modified
    */
    bool merge_table_set(const spb::ClusterState & peer,
        spb::ClusterState & local) const;

private:

    typedef boost::unordered_map<core::uuid,  table_ptr_t> uuid_index_t;
    typedef boost::unordered_map<std::string, table_ptr_t> name_index_t;

    uuid_index_t _uuid_index;
    name_index_t _name_index;
};

}
}

#endif

