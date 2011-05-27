
#ifndef SAMOA_SERVER_TABLE_HPP
#define SAMOA_SERVER_TABLE_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include "samoa/persistence/data_type.hpp"
#include "samoa/core/uuid.hpp"
#include <boost/unordered_set.hpp>
#include <boost/unordered_map.hpp>
#include <vector>

namespace samoa {
namespace server {

namespace spb = samoa::core::protobuf;

class table
{
public:

    typedef table_ptr_t ptr_t;

    typedef std::vector<partition_ptr_t> ring_t;

    //! Constructs a runtime table from protobuf description
    /*!
        \param current The table instance which this table will be replacing
        \sa cluster_state
    */
    table(const spb::ClusterState::Table &,
        const core::uuid & server_uuid,
        const ptr_t & current);

    ~table();

    const core::uuid & get_uuid() const;

    persistence::data_type get_data_type() const;

    const std::string & get_name() const;

    size_t get_replication_factor() const;

    const ring_t & get_ring() const;

    partition_ptr_t get_partition(const core::uuid &) const;

    //! Routes a key to set of partitions responsible for it
    /*!
        The arity of returned partitions will be the minumum of the
        table replication-factory, and the number of live partitions

        \param key Key to be routed
        \param out (Output) responsible partitions
    */
    void route_key(const std::string & key, ring_t & out) const;

    //! Merges a peer table description into the local description
    /*!
        \return true iff local_table was modified
    */
    bool merge_table(const spb::ClusterState::Table & peer_table,
        spb::ClusterState::Table & local_table) const;

private:

    typedef boost::unordered_map<core::uuid, partition_ptr_t> uuid_index_t;

    core::uuid _uuid;
    core::uuid _server_uuid;
    persistence::data_type _data_type;
    std::string _name;
    size_t _repl_factor;

    ring_t        _ring;
    uuid_index_t  _index;
};

}
}

#endif

