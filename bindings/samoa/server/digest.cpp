#include "pysamoa/boost_python.hpp"
#include "samoa/server/digest.hpp"
#include "samoa/core/memory_map.hpp"

namespace samoa {
namespace server {

namespace bpl = boost::python;

core::murmur_checksum_t tuple_to_checksum(const bpl::tuple & t)
{
    if(bpl::len(t) != 2)
    {
        throw std::runtime_error("Expected (unsigned int, unsigned int)");
    }

    core::murmur_checksum_t checksum;
    checksum[0] = bpl::extract<uint64_t>(t[0])();
    checksum[1] = bpl::extract<uint64_t>(t[1])();
    return checksum;
}

void py_add(digest & d, const bpl::tuple & t)
{
    d.add(tuple_to_checksum(t));
}

bool py_test(digest & d, const bpl::tuple & t)
{
    return d.test(tuple_to_checksum(t));
}

const core::memory_map * py_get_memory_map(digest & d)
{
    return d.get_memory_map().get();
}

std::string py_get_directory()
{
    return digest::get_directory().string();
}

void py_set_directory(const std::string & directory)
{
    digest::set_directory(directory);
}

void make_digest_bindings()
{
    bpl::class_<digest, digest_ptr_t, boost::noncopyable>(
            "Digest", bpl::no_init)
        .def("add", &py_add)
        .def("test", &py_test)
        .def("get_properties", &digest::get_properties,
            bpl::return_internal_reference<>())
        .def("get_memory_map", &py_get_memory_map,
            bpl::return_internal_reference<>())
        .def("get_directory", &py_get_directory)
        .staticmethod("get_directory")
        .def("set_directory", &py_set_directory)
        .staticmethod("set_directory")
        .def("get_default_byte_length", &digest::get_default_byte_length)
        .staticmethod("get_default_byte_length")
        .def("set_default_byte_length", &digest::set_default_byte_length)
        .staticmethod("set_default_byte_length")
        ;
}

}
}

