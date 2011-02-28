
#include "samoa/server/context.hpp"
#include "samoa/client/server_pool.hpp"
#include <boost/smart_ptr/make_shared.hpp>

namespace samoa {
namespace server {

context::context(core::proactor::ptr_t p)
 : _proactor(p),
   _peer_pool(boost::make_shared<samoa::client::server_pool>(p))
{ }

}
}

