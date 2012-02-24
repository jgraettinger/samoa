
#include <boost/python.hpp>
#include "samoa/core/memory_map.hpp"

namespace samoa {
namespace core {

namespace bpl = boost::python;

std::string py_get_path(const memory_map & m)
{
	return m.get_path().string();
}

void make_memory_map_bindings()
{
    bpl::class_<memory_map, boost::noncopyable>(
            "MemoryMap", bpl::no_init)
        .def("was_resized", &memory_map::was_resized)
        .def("get_region_size", &memory_map::get_region_size)
        .def("get_path", &py_get_path)
        .def("close", &memory_map::close)
        ;
}

}
}

