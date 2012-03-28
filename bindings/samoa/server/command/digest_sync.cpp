#include <boost/python.hpp>
#include "samoa/server/command/digest_sync_handler.hpp"

namespace samoa {
namespace server {
namespace command {

namespace bpl = boost::python;

void make_digest_sync_handler_bindings()
{
    bpl::class_<digest_sync_handler, digest_sync_handler::ptr_t,
            boost::noncopyable, bpl::bases<command_handler>
        >("DigestSyncHandler", bpl::init<>())
        ;
}

}
}
}
