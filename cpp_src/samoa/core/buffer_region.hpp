#ifndef SAMOA_CORE_BUFFER_REGION
#define SAMOA_CORE_BUFFER_REGION

#include "samoa/core/ref_buffer.hpp"
#include <boost/asio.hpp>

namespace samoa {
namespace core {

class const_buffer_region;

class buffer_region
{
public:

    buffer_region(
        const ref_buffer::ptr_t & buffer,
        size_t min_offset,
        size_t max_offset
    ) :
        _begin(buffer->data() + min_offset),
        _end(buffer->data() + max_offset),
        _buffer(buffer)
    { }

    buffer_region(char * begin, char * end)
     : _begin(begin),
       _end(end)
    { }

    size_t size() const
    { return _end - _begin; }

    char * begin() const
    { return _begin; }

    char * end() const
    { return _end; }

    char getc()
    { return _begin != _end ? *(_begin++) : '\0'; }

    char rgetc()
    { return _begin != _end ? *(--_end) : '\0'; }

    operator boost::asio::mutable_buffer() const
    { return boost::asio::mutable_buffer(begin(), size()); }

private:

    char * _begin, * _end;
    ref_buffer::ptr_t _buffer;
    friend class const_buffer_region;
};


class const_buffer_region
{
public:

    const_buffer_region(
        const ref_buffer::ptr_t & buffer,
        size_t min_offset,
        size_t max_offset
    ) :
        _begin(buffer->data() + min_offset),
        _end(buffer->data() + max_offset),
        _buffer(buffer)
    { }

    const_buffer_region(
        const buffer_region & o
    ) :
        _begin(o._begin),
        _end(o._end),
        _buffer(o._buffer)
    { }

    explicit const_buffer_region(
        const char * str
    ) :
        _begin(str),
        _end(str + strlen(str)),
        _buffer()
    { }

    size_t size() const
    { return _end - _begin; }

    const char * begin() const
    { return _begin; }

    const char * end() const
    { return _end; }

    operator boost::asio::const_buffer() const
    { return boost::asio::const_buffer(begin(), size()); }

private:

    const char * _begin, * _end;
    ref_buffer::ptr_t _buffer;
};

typedef std::vector<buffer_region> buffer_regions_t;
typedef std::vector<const_buffer_region> const_buffer_regions_t;

typedef boost::asio::buffers_iterator<buffer_regions_t> buffers_iterator_t;
typedef boost::asio::buffers_iterator<const_buffer_regions_t> const_buffers_iterator_t;

void copy_regions_into(const buffer_regions_t &, std::string & out);

}
}

#endif
