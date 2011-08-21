#ifndef SAMOA_DATAMODEL_CLUSTER_CLOCK_HPP
#define SAMOA_DATAMODEL_CLUSTER_CLOCK_HPP

#include "samoa/core/protobuf/fwd.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include "samoa/core/uuid.hpp"
#include <string>

namespace samoa {
namespace datamodel {

namespace spb = samoa::core::protobuf;

class clock_util
{
public:

    static void tick(spb::ClusterClock &, const core::uuid & partition_uuid);

    static void validate(const spb::ClusterClock &);

    enum clock_ancestry
    {
        EQUAL = 0,
        MORE_RECENT = 1,
        LESS_RECENT = 2,
        DIVERGE = 3
    };

    static clock_ancestry compare(
        const spb::ClusterClock & lhs, const spb::ClusterClock & rhs,
        spb::ClusterClock * merged_clock_out = 0);
};

}
}

#endif

