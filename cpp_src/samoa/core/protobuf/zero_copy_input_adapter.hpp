#ifndef SAMOA_CORE_PROTOBUF_ZERO_COPY_INPUT_ADAPTER_HPP
#define SAMOA_CORE_PROTOBUF_ZERO_COPY_INPUT_ADAPTER_HPP

#include "samoa/core/ref_buffer.hpp"
#include "samoa/core/buffer_region.hpp"
#include <google/protobuf/io/zero_copy_stream.h>

namespace samoa {
namespace core {
namespace protobuf {

class zero_copy_input_adapter :
    public google::protobuf::io::ZeroCopyInputStream
{
public:

    zero_copy_input_adapter(const buffer_regions_t & buffers)
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
}

#endif

