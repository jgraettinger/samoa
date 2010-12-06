
#include <boost/python.hpp>

namespace core {
    void make_proactor_bindings();
};

BOOST_PYTHON_MODULE(_core)
{
    core::make_proactor_bindings();
}

