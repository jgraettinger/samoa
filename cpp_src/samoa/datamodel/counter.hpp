#ifndef SAMOA_DATAMODEL_COUNTER_HPP
#define SAMOA_DATAMODEL_COUNTER_HPP

#include "samoa/datamodel/merge_func.hpp"
#include "samoa/request/fwd.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include "samoa/core/uuid.hpp"

namespace samoa {
namespace datamodel {

namespace spb = samoa::core::protobuf;

class counter
{
public:

    static void update(spb::PersistedRecord &,
        const core::uuid & partition_uuid,
        int64_t increment);

    static bool prune(spb::PersistedRecord &,
        unsigned consistency_horizon);

    static merge_result merge(
        spb::PersistedRecord & local_record,
        const spb::PersistedRecord & remote_record,
        unsigned consistency_horizon);

    static int64_t value(const spb::PersistedRecord &);
};

}
}

#endif

