
#include <boost/python.hpp>

namespace samoa {
namespace core {
    void make_proactor_bindings();
    void make_future_bindings();
    void make_stream_protocol_bindings();
};
};

BOOST_PYTHON_MODULE(_core)
{
    samoa::core::make_proactor_bindings();
    samoa::core::make_future_bindings();
    samoa::core::make_stream_protocol_bindings();
}

