
#ifndef SAMOA_SERVER_REMOTE_PARTITION_HPP
#define SAMOA_SERVER_REMOTE_PARTITION_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/server/partition.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include <boost/shared_ptr.hpp>

namespace samoa {
namespace server {

namespace spb = samoa::core::protobuf;

class remote_partition : public partition
{
public:

    typedef remote_partition_ptr_t ptr_t;

    //! Constructs a runtime remote_partition from protobuf description
    /*!
        \param current The remote_partition which this instance will
            be replacing. May be nullptr if there is none.
    */
    remote_partition(
        const spb::ClusterState::Table::Partition &,
        uint64_t range_begin, uint64_t range_end,
        const ptr_t & current);

    bool merge_partition(
        const spb::ClusterState::Table::Partition & peer,
        spb::ClusterState::Table::Partition & local) const;
};

}
}

#endif

