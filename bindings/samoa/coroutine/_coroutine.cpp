
#include <boost/python.hpp>

namespace samoa {
namespace coroutine {
    void make_future_bindings();
};
};

BOOST_PYTHON_MODULE(_coroutine)
{
    samoa::coroutine::make_future_bindings();
}

