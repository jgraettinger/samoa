
#include "samoa/server/listener.hpp"
#include "samoa/server/context.hpp"
#include "samoa/server/protocol.hpp"
#include <boost/python.hpp>

namespace samoa {
namespace server {

using namespace boost::python;

void make_listener_bindings()
{
    class_<listener, listener::ptr_t, boost::noncopyable>(
        "Listener", init<std::string, std::string, unsigned,
            context::ptr_t, protocol::ptr_t>(args("host", "port",
            "listen_backlog", "context", "protocol")))
        .def("cancel", &listener::cancel);
}

}
}

