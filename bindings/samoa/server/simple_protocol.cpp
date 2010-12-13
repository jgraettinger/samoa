
#include "samoa/server/simple_protocol.hpp"
#include <boost/python.hpp>

namespace samoa {
namespace server {

using namespace boost::python;

void make_simple_protocol_bindings()
{
    class_<simple_protocol, simple_protocol::ptr_t, boost::noncopyable,
        bases<protocol> >("SimpleProtocol", init<>());
}


}
}

