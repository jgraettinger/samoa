
#ifndef SAMOA_SERVER_TABLE_HPP
#define SAMOA_SERVER_TABLE_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/persistence/data_type.hpp"
#include "samoa/core/fwd.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
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

    unsigned get_replication_factor() const;

    const ring_t & get_ring() const;

    /// Returns nullptr if none exists
    partition_ptr_t get_partition(const core::uuid &) const;

    /// The key's position on the hash-ring continuum
    uint64_t ring_position(const std::string & key) const;

    /*! \brief Routes a position to the set of accountable partitions

    \param ring_position Ring position to route
    \param primary_partition_out The primary responsible partition
            (returned by reference, see notes)
    \param all_partitions_out All responsible partitions
            (returned by reference)
    \return Whether the primary partition is local or remote

    The arity of returned partitions will be the minumum of the
     table replication-factory, and the number of live partitions.
    The primary partition is local iff a local partition is available;
     otherwise it's the first remote partition from the lowest-latency peer,
     iff a connected peer is available. Failing that, it's nullptr.
    If the primary partition is local, then result partitions are
     the exact set of partitions responsible for the key.
    Otherwise, returned partitions are all remote, and are guaraneteed
     only to be 'closer' to those actually accountable for the key.
    (See the Chord routing protocol for further background).
    */
    bool route_ring_position(
        uint64_t ring_position, const peer_set_ptr_t &,
        partition_ptr_t & primary_partition_out,
        ring_t & all_partitions_out) const;

    //! Launches all tasklets required by the runtime table
    void spawn_tasklets(const context_ptr_t &);

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
    unsigned _repl_factor;

    ring_t        _ring;
    uuid_index_t  _index;
};

}
}

#endif

