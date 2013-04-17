#include "pysamoa/boost_python.hpp"
#include "samoa/server/command/cluster_state_handler.hpp"

namespace samoa {
namespace server {
namespace command {

namespace bpl = boost::python;

void make_cluster_state_handler_bindings()
{
    bpl::class_<cluster_state_handler, cluster_state_handler::ptr_t,
            boost::noncopyable, bpl::bases<command_handler>
        >("ClusterStateHandler", bpl::init<>())
        ;
}

}
}
}

