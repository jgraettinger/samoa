
#ifndef SAMOA_PERSISTENCE_FWD_HPP
#define SAMOA_PERSISTENCE_FWD_HPP

#include "samoa/core/protobuf/samoa.pb.h"
#include <memory>

namespace samoa {
namespace persistence {

class persister;
typedef std::shared_ptr<persister> persister_ptr_t;
typedef std::weak_ptr<persister> persister_weak_ptr_t;

}
}

#endif

