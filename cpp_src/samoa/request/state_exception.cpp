#include "samoa/request/state_exception.hpp"

namespace samoa {
namespace request {

state_exception::state_exception(unsigned code, const std::string & err)
 : std::runtime_error(err),
   _code(code)
{ }

}
}

