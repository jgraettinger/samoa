
#ifndef SAMOA_PERSISTENCE_FWD_HPP
#define SAMOA_PERSISTENCE_FWD_HPP

#include "samoa/core/protobuf/samoa.pb.h"
#include <boost/smart_ptr/enable_shared_from_this.hpp>
#include <boost/smart_ptr/make_shared.hpp>
#include <boost/shared_ptr.hpp>

namespace samoa {
namespace persistence {

class persister;
typedef boost::shared_ptr<persister> persister_ptr_t;
typedef boost::weak_ptr<persister> persister_weak_ptr_t;

}
}

#endif

