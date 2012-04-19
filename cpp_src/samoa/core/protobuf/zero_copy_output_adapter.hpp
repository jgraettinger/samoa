#ifndef SAMOA_CORE_PROTOBUF_ZERO_COPY_OUTPUT_ADAPTER_HPP
#define SAMOA_CORE_PROTOBUF_ZERO_COPY_OUTPUT_ADAPTER_HPP

#include "samoa/core/ref_buffer.hpp"
#include "samoa/core/buffer_region.hpp"
#include <google/protobuf/io/zero_copy_stream.h>

namespace samoa {
namespace core {
namespace protobuf {

// TODO find a common location for this
#define ALLOC_BUF_SIZE 10

class zero_copy_output_adapter :
    public google::protobuf::io::ZeroCopyOutputStream
{
public:

    zero_copy_output_adapter()
      : _count(0)
    { }

    // ZeroCopyOutputStream virtual
    bool Next(void ** data, int * size)
    {
        if(_active_buf)
        {
            _buf_regions.push_back(buffer_region(
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
        _buf_regions.push_back(buffer_region(
            _active_buf, 0, _active_buf->size() - count));
        _active_buf.reset();
        return;
    }

    // ZeroCopyOutputStream virtual
    google::protobuf::int64 ByteCount() const
    { return _count; }

    buffer_regions_t & output_regions()
    {
        if(_active_buf)
        {
            // 'flush' remaining active buffer; required
            //   only if BackUp() didn't get called
            _buf_regions.push_back(buffer_region(
                _active_buf, 0, _active_buf->size()));
            _active_buf.reset();
        }
        return _buf_regions;
    }

private:

    unsigned _count;
    ref_buffer::ptr_t _active_buf;
    buffer_regions_t _buf_regions;
};

}
}
}

#endif

