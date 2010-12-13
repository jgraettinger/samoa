
#include <boost/python.hpp>

namespace samoa {
    void make_rolling_hash_bindings();
    void make_heap_rolling_hash_bindings();
    void make_mapped_rolling_hash_bindings();
};

BOOST_PYTHON_MODULE(_samoa)
{
    samoa::make_rolling_hash_bindings();
    samoa::make_heap_rolling_hash_bindings();
    samoa::make_mapped_rolling_hash_bindings();
}

