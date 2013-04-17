#include "pysamoa/boost_python.hpp"

namespace samoa {
namespace persistence {
namespace rolling_hash {
    void make_packet_bindings();
    void make_element_bindings();
    void make_hash_ring_bindings();
    void make_mapped_hash_ring_bindings();
    void make_heap_hash_ring_bindings();
}
}
}

BOOST_PYTHON_MODULE(_rolling_hash)
{
    samoa::persistence::rolling_hash::make_packet_bindings();
    samoa::persistence::rolling_hash::make_element_bindings();
    samoa::persistence::rolling_hash::make_hash_ring_bindings();
    samoa::persistence::rolling_hash::make_mapped_hash_ring_bindings();
    samoa::persistence::rolling_hash::make_heap_hash_ring_bindings();
}

