
#include "samoa/server/context.hpp"
#include <boost/smart_ptr/make_shared.hpp>

namespace samoa {
namespace server {

context::context(core::proactor::ptr_t proactor)
 : _proactor(proactor)
{ }

}
}

