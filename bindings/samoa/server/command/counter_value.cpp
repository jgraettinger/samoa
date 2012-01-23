
#include <boost/python.hpp>
#include "samoa/server/command/counter_value.hpp"

namespace samoa {
namespace server {
namespace command {

namespace bpl = boost::python;

void make_counter_value_handler_bindings()
{
    bpl::class_<counter_value_handler, counter_value_handler::ptr_t,
            boost::noncopyable, bpl::bases<command_handler>
        >("CounterValueHandler", bpl::init<>())
        ;
}

}
}
}

