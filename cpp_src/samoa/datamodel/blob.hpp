#ifndef SAMOA_DATAMODEL_BLOB_HPP
#define SAMOA_DATAMODEL_BLOB_HPP

#include "samoa/request/client_state.hpp"
#include "samoa/datamodel/merge_func.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include <string>

namespace samoa {
namespace datamodel {

namespace spb = samoa::core::protobuf;

class blob
{
public:

    static void send_blob_value(request::client_state &,
        const samoa::core::protobuf::PersistedRecord &);

    static merge_result consistent_merge(
        spb::PersistedRecord & local_record,
        const spb::PersistedRecord & remote_record,
        unsigned consistency_horizon);
};

}
}

#endif

