
#ifndef PYSAMOA_FWD_HPP
#define PYSAMOA_FWD_HPP

#include <memory>

namespace pysamoa {

class coroutine;
typedef std::shared_ptr<coroutine> coroutine_ptr_t;

class future;
typedef std::shared_ptr<future> future_ptr_t;

}

#endif
