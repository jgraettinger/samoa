#ifndef SAMOA_DATAMODEL_BLOB_HPP
#define SAMOA_DATAMODEL_BLOB_HPP

#include "samoa/persistence/record.hpp"
#include "samoa/server/fwd.hpp"
#include "samoa/datamodel/merge_func.hpp"
#include "samoa/core/buffer_region.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include <string>

namespace samoa {
namespace datamodel {

namespace spb = samoa::core::protobuf;

class blob
{
public:

    static void send_blob_value(
        const server::request_state_ptr_t &,
        const samoa::core::protobuf::PersistedRecord &);

    static merge_result consistent_merge(
        spb::PersistedRecord & local_record,
        const spb::PersistedRecord & remote_record,
        unsigned consistency_horizon);
};

}
}

#endif

