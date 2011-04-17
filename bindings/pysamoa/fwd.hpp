
#ifndef PYSAMOA_FWD_HPP
#define PYSAMOA_FWD_HPP

#include <boost/shared_ptr.hpp>
#include <boost/smart_ptr/enable_shared_from_this.hpp>
#include <boost/smart_ptr/make_shared.hpp>

namespace pysamoa {

class coroutine;
typedef boost::shared_ptr<coroutine> coroutine_ptr_t;

class future;
typedef boost::shared_ptr<future> future_ptr_t;

}

#endif
