
#include "samoa/core/proactor.hpp"

namespace samoa {
namespace core {

// boost::asio uses RTTI to establish a common registry of services,
//  however RTTI doesn't work well across shared library boundaries.
//  placing the ctor here guarentees that the member _io_services is
//  constructed in the context of libsamoa
proactor::proactor()
{ }

}
}

