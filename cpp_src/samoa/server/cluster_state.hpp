
#ifndef SAMOA_SERVER_CLUSTER_STATE_HPP
#define SAMOA_SERVER_CLUSTER_STATE_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include "samoa/core/uuid.hpp"

namespace samoa {
namespace server {

namespace spb = samoa::core::protobuf;

//! Represents an immutable, atomic snapshot of the cluster shape and
//!   configuration at a point in time.
/*!
    Samoa cluster configuration can change at any time, but on each change
    a new cluster_state (and peer_set & table_set) instances are built.

    The previous cluster_state remains useable until no references remain.

    Thus, to guarentee a consistent view of the cluster configuration,
    operations should reference the current cluster_state at operation start,
    and use that reference for the operation duration.

    Particularly long-running operations should periodically release and
    re-aquire a cluster_state reference, checking that the operation still
    makes sense. 
*/
class cluster_state
{
public:

    typedef cluster_state_ptr_t ptr_t;

    //! Constructs a runtime cluster_state from protobuf description
    /*!
        \param desc The local cluster_state description
        \param current The cluster_state which this instance will be replacing
            May be nullptr, in which case there is none.
    */
    cluster_state(std::unique_ptr<spb::ClusterState> && desc,
        const ptr_t & current);

    const spb::ClusterState & get_protobuf_description() const
    { return *_desc; }

    const peer_set_ptr_t & get_peer_set() const
    { return _peer_set; }

    const table_set_ptr_t & get_table_set() const
    { return _table_set; }

    //! Merges a peer cluster_state description into the local description
    /*!
        \return true iff local_cluster_state was modified
    */
    bool merge_cluster_state(
        const spb::ClusterState & peer_state,
        spb::ClusterState & local_state) const;

private:

    std::unique_ptr<spb::ClusterState> _desc;

    peer_set_ptr_t _peer_set;
    table_set_ptr_t _table_set;
};

}
}

#endif

