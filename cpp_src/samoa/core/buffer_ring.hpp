#ifndef SAMOA_CORE_BUFFER_RING
#define SAMOA_CORE_BUFFER_RING

#include "samoa/core/ref_buffer.hpp"
#include "samoa/core/buffer_region.hpp"

namespace samoa {
namespace core {

class buffer_ring
{
public:

    buffer_ring()
     : _r_pos(0),
       _w_ind(0), _w_pos(0),
       _r_avail(0), _w_avail(0)
    { }

    size_t available_read() const
    { return _r_avail; }

    size_t available_write() const
    { return _w_avail; }

    // Returns a sequence of buffer regions holding consumable buffer
    template<typename BufferRegion>
    void get_read_regions(
        std::vector<BufferRegion> & regions,
        size_t max_total_size = (size_t)-1
    )
    {
        for(size_t ind = 0; ind != _buffers.size() && max_total_size; ++ind)
        {
            size_t b_begin = ind ? 0 : _r_pos;
            size_t b_end   = ind == _w_ind ? _w_pos : _buffers[ind]->size();

            if((b_end - b_begin) > max_total_size)
                b_end = b_begin + max_total_size;

            regions.push_back(
                BufferRegion(
                    _buffers[ind],
                    b_begin, b_end
                )
            );
            max_total_size -= (b_end - b_begin);

            if(ind == _w_ind) break;
        }
        return;
    }

    // Returns a sequence of buffer regions which may be written into
    template<typename BufferRegion>
    void get_write_regions(
        std::vector<BufferRegion> & regions,
        size_t max_total_size = (size_t)-1
    )
    {
        for(size_t ind = _w_ind; ind != _buffers.size() && max_total_size; ++ind)
        {
            size_t b_begin = ind == _w_ind ? _w_pos : 0;
            size_t b_end   = _buffers[ind]->size();

            if((b_end - b_begin) > max_total_size)
                b_end = b_begin + max_total_size;

            regions.push_back(
                BufferRegion(
                    _buffers[ind],
                    b_begin, b_end
                )
            );
            max_total_size -= (b_end - b_begin);
        }
        return;
    }

    // Allocates buffers such that the next call to get_write_regions
    //  will return writable regions totalling at least write_size
    void reserve(size_t write_size)
    {
        while(_w_avail < write_size)
        {
            _buffers.push_back( ref_buffer::aquire_ref_buffer(2));
            //_buffers.push_back( ref_buffer::aquire_ref_buffer(4064));
            _w_avail += _buffers.back()->size();
        }
        return;
    }

    // Notifies the buffer_ring that read_size bytes have been consumed
    // from regions previously returned by a get_read_regions() call
    void consumed(size_t read_size)
    {
        assert(read_size <= _r_avail);
        _r_avail -= read_size;

        size_t ind = 0;
        while(read_size)
        {
            if((_buffers[ind]->size() - _r_pos) > read_size)
            {
                _r_pos += read_size;
                read_size = 0;
            }
            else
            {
                read_size -= (_buffers[ind]->size() - _r_pos);
                _r_pos = 0;
                ++ind;
            }
        }

        assert( _w_ind > ind || (_w_ind == ind && _w_pos >= _r_pos));

        // remove expired buffers
        _buffers.erase(_buffers.begin(), _buffers.begin() + ind);
        _w_ind -= ind;

        return;
    }

    // Notifies the buffer_ring that write_size bytes have been written
    //  into regions previously returned by a get_write_regions() call
    void produced(size_t write_size)
    {
        assert(write_size <= _w_avail);
        _w_avail -= write_size;
        _r_avail += write_size;

        while(write_size)
        {
            assert(_w_ind != _buffers.size());

            if((_buffers[_w_ind]->size() - _w_pos) > write_size)
            {
                _w_pos += write_size;
                write_size = 0;
            }
            else
            {
                write_size -= (_buffers[_w_ind]->size() - _w_pos);
                _w_pos = 0;
                ++_w_ind;
            }
        }
        return;
    }

    // Writes the sequence into the buffer_ring, allocating
    //  as required to store the complete sequence
    template<typename Iterator>
    void produce_range(const Iterator & begin, const Iterator & end)
    {
        size_t range_len = std::distance(begin, end);
        reserve(range_len);

        size_t ind = _w_ind, pos = _w_pos;
        size_t rem = range_len;

        Iterator cur(begin);
        while(cur != end)
        {
            if(pos == _buffers[ind]->size())
            {
                ind += 1; pos = 0;
                assert(ind != _buffers.size());
            }

            size_t i = std::min(
                rem,
                _buffers[ind]->size() - pos
            );

            std::copy(cur, cur + i, _buffers[ind]->data() + pos);

            cur += i;
            pos += i;
            rem -= i;
        }

        produced(range_len);
        return;
    }

private:

    std::vector<ref_buffer::ptr_t> _buffers;

    size_t _r_pos;
    size_t _w_ind, _w_pos;
    size_t _r_avail, _w_avail;
};

inline std::ostream & operator << (std::ostream & s, const buffer_regions_t & r)
{
    for(auto it = r.begin(); it != r.end(); ++it)
    {
        s.write(it->begin(), it->size());
    }
    return s;
}
}
}

#endif

