
#ifndef SAMOA_SERVER_LOCAL_PARTITION_HPP
#define SAMOA_SERVER_LOCAL_PARTITION_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/server/partition.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include <boost/shared_ptr.hpp>

namespace samoa {
namespace server {

namespace spb = samoa::core::protobuf;

class local_partition : public partition
{
public:

    typedef local_partition_ptr_t ptr_t;

    //! Constructs a runtime local_partition from protobuf description
    /*!
        \param current The local_partition which this instance will
            be replacing. May be nullptr if there is none.
    */
    local_partition(const spb::ClusterState::Table::Partition &,
        const ptr_t & current);

    bool merge_partition(const spb::ClusterState::Table::Partition & peer,
        spb::ClusterState::Table::Partition & local);
};

}
}

#endif

