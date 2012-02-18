#include <boost/python.hpp>
#include "samoa/core/murmur_hash.hpp"
//#include "MurmurHash3.h"

namespace samoa {
namespace core {

namespace bpl = boost::python;

void py_process_bytes(murmur_hash & mh, const bpl::str & input)
{
    char * buf;
    Py_ssize_t len;

    if(PyString_AsStringAndSize(input.ptr(), &buf, &len) == -1)
        bpl::throw_error_already_set();

    mh.process_bytes(reinterpret_cast<const uint8_t*>(buf), len);
}

bpl::tuple py_checksum(const murmur_hash & mh)
{
    std::array<uint64_t, 2> result = mh.checksum();
    return bpl::make_tuple(result[0], result[1]);
}

/*
// Used for testing/comparing against original implementation
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

void make_murmur_hash_bindings()
{
    bpl::class_<murmur_hash>("MurmurHash", bpl::init<>())
        .def(bpl::init<uint32_t>())
        .def("process_bytes", &py_process_bytes)
        .def("checksum", &py_checksum)
        //.def("_original", &py_original)
        //.staticmethod("_orig_algo")
        ;
}

}
}
