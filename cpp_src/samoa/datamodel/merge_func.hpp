#ifndef SAMOA_DATAMODEL_MERGE_FUNC_HPP
#define SAMOA_DATAMODEL_MERGE_FUNC_HPP

#include "samoa/core/protobuf/fwd.hpp"
#include <functional>

namespace samoa {
namespace datamodel {

struct merge_result
{
    merge_result()
     :  local_was_updated(false),
        remote_is_stale(false)
    { }

    merge_result(bool local_was_updated, bool remote_is_stale)
     :  local_was_updated(local_was_updated),
        remote_is_stale(remote_is_stale)
    { }

    bool local_was_updated;
    bool remote_is_stale;
};

typedef std::function<
    merge_result (
        core::protobuf::PersistedRecord &, // local record
        const core::protobuf::PersistedRecord &) // remote record
> merge_func_t;

typedef std::function<
    bool (core::protobuf::PersistedRecord &)
> prune_func_t;

}
}

#endif

