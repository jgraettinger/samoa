
#include <boost/python.hpp>
#include "samoa/core/memory_map.hpp"

namespace samoa {
namespace core {

namespace bpl = boost::python;

std::string py_get_path(const memory_map & m)
{
	return m.get_path().string();
}

bpl::str py_raw_region_str(const memory_map & m)
{
    bpl::str buffer(bpl::handle<>(
        PyString_FromStringAndSize(0, m.get_region_size())));

    void * buffer_ptr = PyString_AS_STRING(buffer.ptr());
    memcpy(buffer_ptr, m.get_region_address(), m.get_region_size());

    return buffer;
}

void make_memory_map_bindings()
{
    bpl::class_<memory_map, boost::noncopyable>(
            "MemoryMap", bpl::no_init)
        .def("get_region_size", &memory_map::get_region_size)
        .def("get_path", &py_get_path)
        .def("raw_region_str", &py_raw_region_str)
        .def("close", &memory_map::close)
        ;
}

}
}

