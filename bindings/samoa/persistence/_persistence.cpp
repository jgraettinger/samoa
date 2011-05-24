
#include <boost/python.hpp>

namespace samoa {
namespace persistence {
    void make_record_bindings();
    void make_rolling_hash_bindings();
    void make_heap_rolling_hash_bindings();
    void make_mapped_rolling_hash_bindings();
    void make_persister_bindings();
    void make_data_type_bindings();
}
}

BOOST_PYTHON_MODULE(_persistence)
{
    samoa::persistence::make_record_bindings();
    samoa::persistence::make_rolling_hash_bindings();
    samoa::persistence::make_heap_rolling_hash_bindings();
    samoa::persistence::make_mapped_rolling_hash_bindings();
    samoa::persistence::make_persister_bindings();
    samoa::persistence::make_data_type_bindings();
}

