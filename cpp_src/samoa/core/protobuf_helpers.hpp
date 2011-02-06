#ifndef SAMOA_CORE_PROTOBUF_HELPERS_HPP
#define SAMOA_CORE_PROTOBUF_HELPERS_HPP

#include "samoa/core/ref_buffer.hpp"
#include "samoa/core/buffer_region.hpp"
#include <google/protobuf/io/zero_copy_stream.h>

namespace samoa {
namespace core {

// TODO find a common location for this
#define ALLOC_BUF_SIZE 10

class zero_copy_output_adapter :
    public google::protobuf::io::ZeroCopyOutputStream
{
public:

    zero_copy_output_adapter()
      : _count(0)
    { }

    void reset()
    {
        // reset for next output operation
        _buf_regions.clear();
        _active_buf.reset();
        _count = 0;
    }

    // ZeroCopyOutputStream virtual
    bool Next(void ** data, int * size)
    {
        if(_active_buf)
        {
            _buf_regions.push_back(const_buffer_region(
                _active_buf, 0, _active_buf->size()));
        }
        _active_buf = ref_buffer::aquire_ref_buffer(ALLOC_BUF_SIZE);
        _count += _active_buf->size();

        // return to protobuf
        *data = _active_buf->data();
        *size = _active_buf->size();
        return true;
    }

    // ZeroCopyOutputStream virtual
    void BackUp(int count)
    {
        if(!_active_buf)
        {
            throw std::runtime_error("zero_copy_output_adapter::BackUp() "
                "No current buffer to back up");
        }
        if(_active_buf->size() < (unsigned)count)
        {
            throw std::runtime_error("zero_copy_output_adapter::BackUp() "
                "Rewind beyond beginning of buffer");
        }

        _count -= count;
        _buf_regions.push_back(const_buffer_region(
            _active_buf, 0, _active_buf->size() - count));
        _active_buf.reset();
        return;
    }

    // ZeroCopyOutputStream virtual
    google::protobuf::int64 ByteCount() const
    { return _count; }

    const_buffer_regions_t & output_regions()
    {
        if(_active_buf)
        {
            // 'flush' remaining active buffer; required
            //   only if BackUp() didn't get called
            _buf_regions.push_back(const_buffer_region(
                _active_buf, 0, _active_buf->size()));
            _active_buf.reset();
        }
        return _buf_regions;
    }

private:

    unsigned _count;
    ref_buffer::ptr_t _active_buf;
    const_buffer_regions_t _buf_regions;
};


class zero_copy_input_adapter :
    public google::protobuf::io::ZeroCopyInputStream
{
public:

    void reset(const buffer_regions_t & buffers)
    {
        _pos = _buf_ind = _buf_pos = 0;
        _buffers = &buffers;
    }

    // ZeroCopyInputStream virtual
    bool Next(const void ** data, int * size)
    {
        if(_buf_ind == _buffers->size())
            return false;

        *data = (*_buffers)[_buf_ind].begin() + _buf_pos;
        *size = (*_buffers)[_buf_ind].size() - _buf_pos;

        _buf_ind += 1;
        _buf_pos = 0;
        _pos += *size;
        return true;
    }

    // ZeroCopyInputStream virtual
    void BackUp(int count)
    {
        // count is less than the size returned
        //   by last call to Next()
        if(_buf_pos == 0)
        {
            _buf_ind -= 1;
            _buf_pos = (*_buffers)[_buf_ind].size();
        }
        assert((unsigned)count <= _buf_pos);
        _buf_pos -= count;
        _pos -= count;
        return;
    }

    // ZeroCopyInputStream virtual
    bool Skip(int count)
    {
        while(_buf_ind != _buffers->size())
        {
            unsigned remainder = (*_buffers)[_buf_ind].size() - _buf_pos;

            if((unsigned)count >= remainder)
            {
                count -= remainder;
                _buf_ind += 1;
                _buf_pos = 0;
                _pos += remainder; 
            }
            else
            {
                _buf_pos += count;
                _pos += count;
                break;
            }
        }
        return (_buf_ind == _buffers->size());
    }

    // ZeroCopyInputStream virtual
    google::protobuf::int64 ByteCount() const
    { return _pos; }

private:

    unsigned _pos;
    unsigned _buf_ind;
    unsigned _buf_pos;

    const buffer_regions_t * _buffers;
};

}
}

#endif

