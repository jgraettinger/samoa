#ifndef SAMOA_PERSISTENCE_ROLLING_HASH_VALUE_ZCI_ADAPTER_HPP
#define SAMOA_PERSISTENCE_ROLLING_HASH_VALUE_ZCI_ADAPTER_HPP

#include <google/protobuf/io/zero_copy_stream.h>

namespace samoa {
namespace persistence {
namespace rolling_hash {

class value_zci_adapter :
    public google::protobuf::io::ZeroCopyInputStream
{
public:

    value_zci_adapter(const element & element)
     :  _element(element),
        _next_packet(_element.head()),
        _next_offset(0),
        _total_bytes(0)
    { }

    bool Next(const void ** data, int * size)
    {
        SAMOA_ASSERT(_next_packet);

        if(_next_offset == _next_packet->value_length())
        {
            _next_packet = _element.step(_next_packet);
            _next_offset = 0;

            if(_next_packet == nullptr)
            {
                // end-of-stream condition
                return false;
            }
        }

        // return remaining content in this packet
        *data = _next_packet->value_begin() + _next_offset;
        *size = _next_packet->value_length() - _next_offset;

        _total_bytes += _next_packet->value_length() - _next_offset;
        _next_offset = _next_packet->value_length();
        return true;
    }

    void BackUp(int count)
    {
        SAMOA_ASSERT(count <= _next_offset);

        _next_offset -= count;
        _total_bytes -= count;
    }

    bool Skip(int count)
    {
        while(count)
        {
            const void * tmp;
            int size;

            if(!Next(&tmp, &size))
                return false;

            if(size > count)
            {
                BackUp(size - count);
                return true;
            }
            count -= size;
        }
    }

    google::protobuf::int64 ByteCount() const
    { return _total_bytes; }

private:

    const element & _element;
    packet * _next_packet;
    unsigned _next_offset;
    unsigned _total_bytes;
};

}
}
}

#endif
