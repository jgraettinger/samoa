
#include <boost/python.hpp>

namespace samoa {
namespace persistence {
    void make_persister_bindings();
}
}

BOOST_PYTHON_MODULE(_persistence)
{
    samoa::persistence::make_persister_bindings();
}

