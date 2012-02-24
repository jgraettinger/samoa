
#include <boost/python.hpp>
#include "samoa/server/local_digest.hpp"
#include "samoa/core/memory_map.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include "samoa/core/buffer_region.hpp"

namespace samoa {
namespace server {

namespace bpl = boost::python;

void make_local_digest_bindings()
{
    bpl::class_<local_digest, bpl::bases<digest>, boost::noncopyable>(
            "LocalDigest", bpl::init<const core::uuid &>())
        ;
}

}
}

