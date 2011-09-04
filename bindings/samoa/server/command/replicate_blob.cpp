
#include <boost/python.hpp>
#include "samoa/server/command/replicate_blob.hpp"

namespace samoa {
namespace server {
namespace command {

namespace bpl = boost::python;

void make_replicate_blob_handler_bindings()
{
    bpl::class_<replicate_blob_handler, replicate_blob_handler::ptr_t, boost::noncopyable,
            bpl::bases<basic_replicate_handler> >("ReplicateBlobHandler", bpl::init<>())
        ;
}

}
}
}

