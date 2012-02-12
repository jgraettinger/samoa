#include <boost/python.hpp>
#include "samoa/core/murmur_checksummer.hpp"
//#include "MurmurHash3.h"

namespace samoa {
namespace core {

namespace bpl = boost::python;

void py_process_bytes(murmur_checksummer & mc, const bpl::str & input)
{
    char * buf;
    Py_ssize_t len;

    if(PyString_AsStringAndSize(input.ptr(), &buf, &len) == -1)
        bpl::throw_error_already_set();

    mc.process_bytes(reinterpret_cast<const uint8_t*>(buf), len);
}

bpl::tuple py_checksum(const murmur_checksummer & mc)
{
    std::array<uint64_t, 2> result = mc.checksum();
    return bpl::make_tuple(result[0], result[1]);
}

/*
// Used for testing against original implementation
bpl::tuple py_original(uint32_t seed, const bpl::str & input)
{
    char * buf;
    Py_ssize_t len;

    if(PyString_AsStringAndSize(input.ptr(), &buf, &len) == -1)
        bpl::throw_error_already_set();

    std::array<uint64_t, 2> result;
    MurmurHash3_x64_128(buf, len, seed, &result);

    return bpl::make_tuple(result[0], result[1]);
}
*/

void make_murmur_checksummer_bindings()
{
    bpl::class_<murmur_checksummer>("MurmurChecksummer", bpl::init<uint32_t>())
        .def("process_bytes", &py_process_bytes)
        .def("checksum", &py_checksum)
        //.def("_original", &py_original)
        //.staticmethod("_orig_algo")
        ;
}

}
}
