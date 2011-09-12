#include <boost/python.hpp>
#include "samoa/server/command/basic_replicate.hpp"

namespace samoa {
namespace server {
namespace command {

namespace bpl = boost::python;

void make_replicate_handler_bindings()
{
    bpl::class_<replicate_handler, replicate_handler::ptr_t, boost::noncopyable,
            bpl::bases<command_handler> >("ReplicateHandler", bpl::init<>())
        ;
}

}
}
}

