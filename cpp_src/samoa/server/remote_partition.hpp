
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

    remote_partition(
        const spb::ClusterState::Table::Partition &,
        uint64_t range_begin, uint64_t range_end, bool is_tracked);

    remote_partition(
        const spb::ClusterState::Table::Partition &,
        uint64_t range_begin, uint64_t range_end, bool is_tracked,
        const remote_partition & current);

    bool merge_partition(
        const spb::ClusterState::Table::Partition & peer,
        spb::ClusterState::Table::Partition & local) const;

    void initialize(const context_ptr_t &, const table_ptr_t &);

    using partition::set_digest;
};

}
}

#endif

