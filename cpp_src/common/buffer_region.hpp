
#ifndef COMMON_BUFFER_REGION
#define COMMON_BUFFER_REGION

#include "common/ref_buffer.hpp"
#include <boost/asio.hpp>

namespace common {

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


typedef std::vector<buffer_region
    > buffer_regions_t;
typedef std::vector<const_buffer_region
    > const_buffer_regions_t;

typedef boost::asio::buffers_iterator<buffer_regions_t
    > buffers_iterator_t;
typedef boost::asio::buffers_iterator<const_buffer_regions_t
    > const_buffers_iterator_t;


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
            _buffers.push_back( ref_buffer::aquire_ref_buffer(1));
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

} // end common

#endif
