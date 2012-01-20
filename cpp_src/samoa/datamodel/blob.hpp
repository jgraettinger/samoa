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

    static void update(spb::PersistedRecord &, uint64_t author_id,
        const core::buffer_regions_t & value);

    static void update(spb::PersistedRecord &, uint64_t author_id,
        const std::string & value); 

    static bool prune(spb::PersistedRecord &,
        unsigned consistency_horizon);

    static merge_result merge(
        spb::PersistedRecord & local_record,
        const spb::PersistedRecord & remote_record,
        unsigned consistency_horizon);

    /*!
     * ValueCallback is a concept with signature void(const std::string &),
     *  which is called for each divergent blob_value of the record
     */
    template<typename ValueCallback>
    static void value(const spb::PersistedRecord &, const ValueCallback &);
};

}
}

#include "samoa/datamodel/blob.impl.hpp"

#endif

