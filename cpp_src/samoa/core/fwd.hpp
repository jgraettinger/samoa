#ifndef SAMOA_CORE_FWD_HPP
#define SAMOA_CORE_FWD_HPP

#include <boost/asio.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/smart_ptr/enable_shared_from_this.hpp>
#include <boost/smart_ptr/make_shared.hpp>

namespace samoa {
namespace core {

class proactor;
typedef boost::shared_ptr<proactor> proactor_ptr_t;

typedef boost::shared_ptr<boost::asio::io_service> io_service_ptr_t;
typedef boost::shared_ptr<boost::asio::deadline_timer> timer_ptr_t;

}
}

#endif

