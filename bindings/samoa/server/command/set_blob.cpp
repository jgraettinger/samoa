#include "pysamoa/boost_python.hpp"
#include "samoa/server/command/set_blob.hpp"

namespace samoa {
namespace server {
namespace command {

namespace bpl = boost::python;

void make_set_blob_handler_bindings()
{
    bpl::class_<set_blob_handler, set_blob_handler::ptr_t, boost::noncopyable,
            bpl::bases<command_handler> >("SetBlobHandler", bpl::init<>())
        ;
}

}
}
}

