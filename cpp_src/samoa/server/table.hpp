
#ifndef SAMOA_SERVER_TABLE_HPP
#define SAMOA_SERVER_TABLE_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/client/fwd.hpp"
#include "samoa/datamodel/data_type.hpp"
#include "samoa/datamodel/merge_func.hpp"
#include "samoa/core/fwd.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include "samoa/core/uuid.hpp"
#include <boost/unordered_map.hpp>
#include <boost/smart_ptr/enable_shared_from_this.hpp>
#include <vector>
#include <list>

namespace samoa {
namespace server {

namespace spb = samoa::core::protobuf;

class table :
    public boost::enable_shared_from_this<table>
{
public:

    typedef table_ptr_t ptr_t;

    typedef std::vector<partition_ptr_t> ring_t;
    typedef boost::unordered_map<core::uuid, partition_ptr_t> uuid_index_t;

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

    datamodel::data_type get_data_type() const;

    const std::string & get_name() const;

    unsigned get_replication_factor() const;

    unsigned get_consistency_horizon() const;

    const ring_t & get_ring() const;

    /// Returns nullptr if none exists
    partition_ptr_t get_partition(const core::uuid &) const;

    const uuid_index_t & get_uuid_index() const
    { return _uuid_index; }

    const datamodel::merge_func_t & get_consistent_merge() const;

    const datamodel::prune_func_t & get_consistent_prune() const;

    /// The key's position on the hash-ring continuum
    static uint64_t ring_position(const std::string & key);

    void initialize(const context_ptr_t &);

    //! Merges a peer table description into the local description
    /*!
        \return true iff local_table was modified
    */
    bool merge_table(const spb::ClusterState::Table & peer_table,
        spb::ClusterState::Table & local_table) const;

    /*!
     *  Given a sorted list of local_parition indicies within
     *  a table ring and a table replication factor, determines
     *  whether the remote_partition at the given index within the
     *  table ring is a 'neighbor' of a local_partition. Eg, the
     *  remote_partition shares a partially overlapping range of
     *  responsiblity with at least one local_partition.
     */
    static bool is_neighbor(
        const std::vector<unsigned> & local_partition_indices,
        unsigned ring_size, unsigned ring_index,
        unsigned replication_factor);

private:

    core::uuid _uuid;
    core::uuid _server_uuid;
    datamodel::data_type _data_type;
    std::string _name;
    unsigned _replication_factor;
    unsigned _consistency_horizon;

    ring_t        _ring;
    uuid_index_t  _uuid_index;

    datamodel::merge_func_t _consistent_merge;
    datamodel::prune_func_t _consistent_prune;
};

}
}

#endif

