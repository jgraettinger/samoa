#include "pysamoa/boost_python.hpp"
#include "samoa/server/command/replicate.hpp"

namespace samoa {
namespace server {
namespace command {

namespace bpl = boost::python;

void make_replicate_handler_bindings()
{
    bpl::class_<replicate_handler, replicate_handler::ptr_t,
            boost::noncopyable, bpl::bases<command_handler>
        >("ReplicateHandler", bpl::init<>())
        ;
}

}
}
}

