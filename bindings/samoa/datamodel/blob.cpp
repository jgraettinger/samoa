
#include <boost/python.hpp>
#include "samoa/datamodel/blob.hpp"
#include "samoa/core/buffer_region.hpp"
#include "samoa/error.hpp"
#include <boost/iostreams/device/array.hpp>
#include <boost/iostreams/stream.hpp>

namespace samoa {
namespace datamodel {

/*
namespace bio = boost::iostreams;
namespace bpl = boost::python;

void py_write_blob_value(
    const cluster_clock & clock,
    const bpl::str & value,
    persistence::record & record_out)
{
    char * buffer = PyString_AS_STRING(value.ptr());

    blob::write_blob_value(
        clock,
        core::buffer_regions_t(1, core::buffer_region(
            buffer, buffer + PyString_GET_SIZE(value.ptr()))),
        record_out);
}

bpl::tuple py_read_blob_value(
    const persistence::record & record)
{
    // build return value
    bpl::manage_new_object::apply<
        cluster_clock *>::type clock_convert;
    bpl::object py_clock(bpl::handle<>(clock_convert(new cluster_clock())));

    bpl::list py_values;

    bio::stream<bio::array_source> istr(
        record.value_begin(), record.value_end());

    // parse cluster clock
    istr >> bpl::extract<cluster_clock &>(py_clock)();

    std::vector<unsigned> value_lengths;
    std::streampos total_length = 0;

    // parse value lengths
    while(true)
    {
        unsigned end_offset = istr.tellg() + total_length;

        if(end_offset == record.value_length())
        {
            break;
        }

        SAMOA_ASSERT(end_offset < record.value_length());

        packed_unsigned blob_length;
        istr >> blob_length;

        value_lengths.push_back(blob_length);
        total_length += blob_length;
    }

    // parse values
    for(auto it = value_lengths.begin(); it != value_lengths.end(); ++it)
    {
        PyObject * py_str = PyString_FromStringAndSize(0, *it);
        py_values.append(bpl::handle<>(py_str));

        istr.read(PyString_AS_STRING(py_str), *it);
    }

    return bpl::make_tuple(py_clock, py_values);
}
*/

void make_blob_bindings()
{
    /*
    bpl::class_<blob>("Blob", bpl::no_init)
        .def("serialized_length", &blob::serialized_length)
        .staticmethod("serialized_length")
        .def("write_blob_value", &py_write_blob_value)
        .staticmethod("write_blob_value")
        .def("read_blob_value", &py_read_blob_value)
        .staticmethod("read_blob_value")
        ;
    */
}

}
}

