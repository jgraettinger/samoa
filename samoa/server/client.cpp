
#include "samoa/server/client.hpp"
#include "samoa/server/context.hpp"
#include "samoa/server/protocol.hpp"
#include "samoa/common/stream_protocol.hpp"
#include <boost/asio.hpp>
#include <boost/python.hpp>

namespace server {

using namespace boost::asio;
using namespace boost::python;

client::client(context::ptr_t context, protocol::ptr_t protocol,
    std::auto_ptr<ip::tcp::socket> sock)
 : common::stream_protocol(sock),
   _context(context),
   _protocol(protocol)
{ }

void client::start_next_request()
{ _protocol->next_request(shared_from_this()); }

void make_client_bindings()
{
    class_<client, client::ptr_t, boost::noncopyable
            //bases<stream_protocol>
            >("client", no_init)
        .def("start_next_request", &client::start_next_request)
        .add_property("context", make_function(&client::get_context,
            return_value_policy<copy_const_reference>()))
        .add_property("protocol", make_function(&client::get_protocol,
            return_value_policy<copy_const_reference>()));
}

}

