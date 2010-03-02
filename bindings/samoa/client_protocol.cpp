
#include "samoa/client_protocol.hpp"
#include <boost/python.hpp>

using namespace samoa;
using namespace boost::python;

void make_client_protocol_bindings()
{
    class_<client_protocol, client_protocol::ptr_t, boost::noncopyable>(
        "client_protocol", no_init);
}
