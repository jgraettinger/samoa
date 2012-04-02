
#include <boost/python.hpp>
#include "samoa/server/remote_digest.hpp"
#include "samoa/core/memory_map.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include "samoa/core/buffer_region.hpp"

namespace samoa {
namespace server {

namespace bpl = boost::python;

void make_remote_digest_bindings()
{
    bpl::class_<remote_digest, bpl::bases<digest>, boost::noncopyable>(
            "RemoteDigest", bpl::init<const core::uuid &, const core::uuid &>())
        .def(bpl::init<const core::uuid &,
            const core::uuid &,
            const spb::DigestProperties &,
            const core::buffer_regions_t &>())
        .def("mark_filter_for_deletion",
            &remote_digest::mark_filter_for_deletion)
        ;
}

}
}

