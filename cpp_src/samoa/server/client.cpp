
#include "samoa/server/client.hpp"
#include "samoa/server/context.hpp"
#include "samoa/server/protocol.hpp"
#include "samoa/core/stream_protocol.hpp"
#include <boost/asio.hpp>

namespace samoa {
namespace server {

using namespace boost::asio;

client::client(context::ptr_t context, protocol::ptr_t protocol,
    std::auto_ptr<ip::tcp::socket> sock)
 : core::stream_protocol(sock),
   _context(context),
   _protocol(protocol)
{ }

void client::start_next_request()
{ _protocol->next_request(shared_from_this()); }

}
}

