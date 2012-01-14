#ifndef SAMOA_DATAMODEL_BLOB_HPP
#define SAMOA_DATAMODEL_BLOB_HPP

#include "samoa/datamodel/merge_func.hpp"
#include "samoa/request/fwd.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include "samoa/core/uuid.hpp"
#include "samoa/core/buffer_region.hpp"
#include <string>

namespace samoa {
namespace datamodel {

namespace spb = samoa::core::protobuf;

class blob
{
public:

    static void update(spb::PersistedRecord &,
        const core::uuid & partition_uuid,
        const core::buffer_regions_t & value);

    static void update(spb::PersistedRecord &,
        const core::uuid & partition_uuid,
        const std::string & value); 

    static bool prune(spb::PersistedRecord &,
        unsigned consistency_horizon);

    static merge_result merge(
        spb::PersistedRecord & local_record,
        const spb::PersistedRecord & remote_record,
        unsigned consistency_horizon);

    static void send_blob_value(const request::state_ptr_t &,
        const spb::PersistedRecord &);
};

}
}

#endif

