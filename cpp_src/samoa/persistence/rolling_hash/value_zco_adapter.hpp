#ifndef SAMOA_PERSISTENCE_ROLLING_HASH_VALUE_ZCI_ADAPTER_HPP
#define SAMOA_PERSISTENCE_ROLLING_HASH_VALUE_ZCI_ADAPTER_HPP

#include "samoa/persistence/rolling_hash/error.hpp"
#include <google/protobuf/io/zero_copy_stream.h>

namespace samoa {
namespace persistence {
namespace rolling_hash {

class value_zco_adapter :
    public google::protobuf::io::ZeroCopyOutputStream
{
public:

    value_zco_adapter(element & element)
     :  _element(element),
        _next_packet(_element.head()),
        _next_offset(0),
        _total_bytes(0)
    { }

    bool Next(const void ** data, int * size)
    {
        SAMOA_ASSERT(_next_packet);

        uint32_t available_length = _next_packet->capacity() \
            - _next_packet->key_length();

        if(_next_offset == available_length)
        {
            // we've completely filled _next_packet
            _next_packet->set_crc_32(_next_packet->compute_crc_32());

            if(_next_packet->completes_sequence())
            {
                // end-of-stream condition
                return false;
            }

            // use hash_ring::next_packet() rather than element::step(),
            //  to skip performing an integrity check, because 1) we
            //  don't require the existing value, 2) if the element is
            //  being constructed the check will fail, and 3) if we're
            //  updating the value, we've recently read it already.
            _next_packet = _element.ring()->next_packet(_next_packet);
            _next_offset = 0;

            RING_INTEGRITY_CHECK(_next_packet->continues_sequence());
        }

        *data = _next_packet->set_value(available_length) + _next_offset;
        *size = available_length - _next_offset;

        _total_bytes += available_length - _next_offset;
        _next_offset = available_length;
        return true;
    }

    void BackUp(int count)
    {
        SAMOA_ASSERT((unsigned)count <= _next_offset);

        // this is generally called when all data is written,
        //   so we'll want to update the _next_packet value-length
        //   (without destroying written data) and compute the crc

        uint32_t new_length = _next_packet->value_length() - count;

        _next_packet->set_value(new_length);
        _next_packet->set_crc_32(_next_packet->compute_crc_32());

        _next_offset -= count;
        _total_bytes -= count;
    }

    google::protobuf::int64 ByteCount() const
    { return _total_bytes; }

    void finish()
    {
        while(_next_packet)
        {
            if(!(_next_packet = _element.step(_next_packet)))
                break;

            _next_packet->set_value(0);
            _next_packet->set_crc_32(_next_packet->compute_crc_32());
        }
    }

private:

    element & _element;
    packet * _next_packet;
    unsigned _next_offset;
    unsigned _total_bytes;
};

}
}
}

#endif
