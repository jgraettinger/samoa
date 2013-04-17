#include "pysamoa/boost_python.hpp"

namespace samoa {
namespace datamodel {
    void make_clock_util_bindings();
    void make_data_type_bindings();
    void make_merge_func_bindings();
    void make_blob_bindings();
    void make_counter_bindings();
}
}

BOOST_PYTHON_MODULE(_datamodel)
{
    samoa::datamodel::make_clock_util_bindings();
    samoa::datamodel::make_data_type_bindings();
    samoa::datamodel::make_merge_func_bindings();
    samoa::datamodel::make_blob_bindings();
    samoa::datamodel::make_counter_bindings();
}

