
#ifndef SAMOA_PERSISTENCE_FWD_HPP
#define SAMOA_PERSISTENCE_FWD_HPP

#include <boost/shared_ptr.hpp>
#include <boost/smart_ptr/enable_shared_from_this.hpp>
#include <boost/smart_ptr/make_shared.hpp>

namespace samoa {
namespace persistence {

class record;

class rolling_hash;
class heap_rolling_hash;
class mapped_rolling_hash;

class persister;
typedef boost::shared_ptr<persister> persister_ptr_t;

}
}

#endif

