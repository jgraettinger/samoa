#ifndef SAMOA_DATAMODEL_BLOB_HPP
#define SAMOA_DATAMODEL_BLOB_HPP

#include "samoa/persistence/record.hpp"
#include "samoa/server/fwd.hpp"
#include "samoa/datamodel/partition_clock.hpp"
#include "samoa/datamodel/packed_unsigned.hpp"
#include "samoa/core/buffer_region.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include <string>

namespace samoa {
namespace datamodel {

/*! 

Blob is serialized as:

    - cluster_clock
    - repeated:
      - unsigned (blob length)
      - repeated byte (blob value)

*/
class blob
{
public:

    static void send_blob_value(
        const server::client_ptr_t &,
        const samoa::core::protobuf::Value &);
};

}
}

#endif

