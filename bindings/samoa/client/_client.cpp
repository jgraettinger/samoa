
#include <boost/python.hpp>

namespace samoa {
namespace client {
    void make_server_bindings();
}
}

BOOST_PYTHON_MODULE(_client)
{
    samoa::client::make_server_bindings();
}

