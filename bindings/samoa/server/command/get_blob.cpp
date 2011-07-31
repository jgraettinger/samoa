
#include <boost/python.hpp>
#include "samoa/server/command/get_blob.hpp"

namespace samoa {
namespace server {
namespace command {

namespace bpl = boost::python;

void make_get_blob_handler_bindings()
{
    bpl::class_<get_blob_handler, get_blob_handler::ptr_t, boost::noncopyable,
            bpl::bases<command_handler> >("GetBlobHandler", bpl::init<>())
        ;
}

}
}
}

