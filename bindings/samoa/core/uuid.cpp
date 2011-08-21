
#include <boost/python.hpp>
#include "samoa/core/uuid.hpp"
#include "samoa/error.hpp"
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/uuid_generators.hpp>

namespace samoa {
namespace core {

namespace bpl = boost::python;

std::string py_to_hex(const uuid & u)
{ return boost::lexical_cast<std::string>(u); }

std::string py_repr(const uuid & u)
{ return "UUID('" + py_to_hex(u) + "')"; }

bool py_check_hex(const bpl::str & s)
{
    char * buf;
    Py_ssize_t len;

    if(PyString_AsStringAndSize(s.ptr(), &buf, &len) == -1)
        bpl::throw_error_already_set();

    try
    {
        boost::uuids::string_generator()(buf, buf + len);
        return true;
    }
    catch(const std::runtime_error & e)
    {
        return false;
    }
}

uuid py_from_hex(const bpl::str & s)
{ 
    char * buf;
    Py_ssize_t len;

    if(PyString_AsStringAndSize(s.ptr(), &buf, &len) == -1)
        bpl::throw_error_already_set();

    boost::uuids::string_generator gen;
    return gen(buf, buf + len);
}

uuid py_from_bytes(const bpl::str & s)
{
    char * buf;
    Py_ssize_t len;

    if(PyString_AsStringAndSize(s.ptr(), &buf, &len) == -1)
        bpl::throw_error_already_set();

    uuid result;

    SAMOA_ASSERT(len == sizeof(result.data));
    std::copy(buf, buf + len, result.data);

    return result;
}

uuid * py_hex_ctor(const bpl::str & s)
{
    // boost::python expects a new reference when this function
    //  is used within boost::python::make_constructor()
    return new uuid(py_from_hex(s));
}

uuid py_from_name(const bpl::str & s)
{
    char * buf;
    Py_ssize_t len;

    if(PyString_AsStringAndSize(s.ptr(), &buf, &len) == -1)
        bpl::throw_error_already_set();

    boost::uuids::nil_generator nil_gen;
    boost::uuids::name_generator name_gen(nil_gen());

    // odd, but name_generator takes a buffer length
    //  while string_generator takes an interation range
    return name_gen(buf, len);
}

uuid py_from_random()
{
    boost::uuids::basic_random_generator<boost::mt19937> gen;
    return gen();
}

uuid py_from_nil()
{
    boost::uuids::nil_generator gen;
    return gen();
}

int py_cmp(const uuid & self, const bpl::object & other)
{
    bpl::extract<const uuid &> ex(other);

    // UUID is 'less' than all other types
    if(!ex.check())
        return -1;

    const uuid & o = ex;

    if(self < o)
        return -1;
    if(self > o)
        return 1;
    return 0;
}

size_t py_hash(const uuid & self)
{ return hash_value(self); }

void make_uuid_bindings()
{
    bpl::class_<uuid>("UUID", bpl::init<const uuid &>())
        .def("__init__", bpl::make_constructor(py_hex_ctor))
        .def("__repr__", &py_repr)
        .def("to_hex", &py_to_hex)
        .def("check_hex", &py_check_hex)
        .staticmethod("check_hex")
        .def("from_hex", &py_from_hex)
        .staticmethod("from_hex")
        .def("from_bytes", &py_from_bytes)
        .staticmethod("from_bytes")
        .def("from_name", &py_from_name)
        .staticmethod("from_name")
        .def("from_random", &py_from_random)
        .staticmethod("from_random")
        .def("from_nil", &py_from_nil)
        .staticmethod("from_nil")
        .def("__cmp__", &py_cmp)
        .def("__hash__", &py_hash);
}

};
};

