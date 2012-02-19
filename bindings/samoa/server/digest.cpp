
#include <boost/python.hpp>
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

void make_digest_bindings()
{
    bpl::class_<digest, digest_ptr_t, boost::noncopyable>(
            "Digest", bpl::init<const core::uuid &>())
        .def("add", &py_add)
        .def("test", &py_test)
        .def("get_path_base", &digest::get_path_base,
            bpl::return_value_policy<bpl::copy_const_reference>())
        .staticmethod("get_path_base")
        .def("set_path_base", &digest::set_path_base)
        .staticmethod("set_path_base")
        .def("get_default_byte_length", &digest::get_default_byte_length)
        .staticmethod("get_default_byte_length")
        .def("set_default_byte_length", &digest::set_default_byte_length)
        .staticmethod("set_default_byte_length")
        ;
}

}
}

