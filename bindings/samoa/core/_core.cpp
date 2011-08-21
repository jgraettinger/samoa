
#include <boost/python.hpp>
#include "samoa/core/protobuf/fwd.hpp"
#include "samoa/core/protobuf/samoa.pb.h"

namespace samoa {
namespace core {
    void make_proactor_bindings();
    void make_stream_protocol_bindings();
    void make_uuid_bindings();
    void make_server_time_bindings();
    void make_tasklet_bindings();
    void make_tasklet_group_bindings();
};
};

BOOST_PYTHON_MODULE(_core)
{
    samoa::core::make_proactor_bindings();
    samoa::core::make_stream_protocol_bindings();
    samoa::core::make_uuid_bindings();
    samoa::core::make_server_time_bindings();
    samoa::core::make_tasklet_bindings();
    samoa::core::make_tasklet_group_bindings();

    boost::python::register_ptr_to_python<
        samoa::core::protobuf::PersistedRecord_ptr_t>();
}

