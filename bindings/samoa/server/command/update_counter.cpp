#include "pysamoa/boost_python.hpp"
#include "samoa/server/command/update_counter.hpp"

namespace samoa {
namespace server {
namespace command {

namespace bpl = boost::python;

void make_update_counter_handler_bindings()
{
    bpl::class_<update_counter_handler, update_counter_handler::ptr_t,
            boost::noncopyable, bpl::bases<command_handler>
        >("UpdateCounterHandler", bpl::init<>())
        ;
}

}
}
}

