#include "pysamoa/boost_python.hpp"

namespace samoa {
namespace request {
    void make_table_state_bindings();
    void make_route_state_bindings();
    void make_record_state_bindings();
    void make_replication_state_bindings();
    void make_state_bindings();
    void make_state_exception_bindings();
}
}

BOOST_PYTHON_MODULE(_request)
{
    samoa::request::make_table_state_bindings();
    samoa::request::make_route_state_bindings();
    samoa::request::make_record_state_bindings();
    samoa::request::make_replication_state_bindings();
    samoa::request::make_state_bindings();
    samoa::request::make_state_exception_bindings();
}

