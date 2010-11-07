#ifndef COMMON_STREAM_PROTOCOL_HPP
#define COMMON_STREAM_PROTOCOL_HPP

#include "common/ref_buffer.hpp"
#include "common/buffer_region.hpp"
#include <boost/regex.hpp>
#include <boost/asio.hpp>
#include <boost/system/error_code.hpp>
#include <boost/bind/protect.hpp>
#include <boost/bind.hpp>
#include <memory>

/*
    stream_protocol provides two kinds of read operations: regex reads,
    and raw data reads.
        
        read_regex() will read from the socket until one of the argument regex
        is matched, or max_match_length has been exceeded. Regex's are matched
        greedily, in sequence order. It takes a callback which is expected to
        have the type signature below (failure to use this type signature will
        result in lots of compiler errors:
        
        regex_callback(
            const boost::system::error_code &,
            const stream_protocol::match_results_t &,
            size_t matched_index
        );
        
        read_data() will read a specified amount of raw bytes from the
        socket. It takes a callback of signature:
        
        data_callback(
            const boost::system::error_code &,
            size_t read_length
        );
*/

namespace common {

class stream_protocol : private boost::noncopyable
{
public:
    
    typedef boost::match_results<buffers_iterator_t
        > match_results_t;
    
    stream_protocol(
        std::auto_ptr<boost::asio::ip::tcp::socket> & sock
    );
    
    // Underlying socket
    boost::asio::ip::tcp::socket & socket()
    { return *_sock; }
    
    // Is currently in a read operation?
    bool in_read() const
    { return _in_read; }
    
    // Is currently in a write operation?
    bool in_write() const
    { return _in_write; }
    
    // Initiates an asynchronous read of any of the
    //  specified regexs from the socket
    template<typename ReIterator, typename Callback>
    void read_regex(
        const  ReIterator & re_begin,
        const  ReIterator & re_end,
        size_t max_match_length,
        const  Callback & callback
    );
    
    // Initiates an asynchronous read of read_length
    //  bytes from the socket. data_regions must have
    //  a lifetime >= the read operation.
    template<typename Callback>
    void read_data(
        size_t read_length,
        buffer_regions_t & data_regions,
        const Callback & callback
    );
    
    // queue_write(*) - schedule buffer for writing to the client,
    //  using gather-IO.
    //
    // NOTE! If there is no current write operation, queue_write
    //  *DOES NOT* start one. Check the value of in_write() to
    //  see if one should be manually started.
    // However, Iff a write operation is in progress, it will
    //  iteratively consume all queued buffer.
    
    void queue_write(const const_buffer_region &);
    void queue_write(const const_buffer_regions_t &);
    
    template<typename Iterator>
    void queue_write(const Iterator & begin, const Iterator & end);
    
    void queue_write(const std::string & str)
    { queue_write(str.begin(), str.end()); }
    
    // Initiates an asynchronous write of all queued buffer.
    //  Will iteratively consume any queued buffer, calling
    //  back only when none remains.
    template<typename Callback>
    void write_queued(const Callback & callback);
    
private:
    
    template<typename ReIterator, typename Callback>
    void on_regex_read(
        const  boost::system::error_code & ec,
        size_t bytes_transferred,
        const  ReIterator & re_begin,
        const  ReIterator & re_end,
        size_t max_match_length,
        const  Callback & callback
    );
    
    template<typename Callback>
    void on_data_read(
        const  boost::system::error_code & ec,
        size_t bytes_transferred,
        size_t read_length,
        buffer_regions_t & data_regions,
        const  Callback & callback
    );
    
    template<typename Callback>
    void on_write(
        const  boost::system::error_code & ec,
        size_t bytes_transferred,
        const  Callback & callback
    );
    
    void pre_socket_read(size_t read_target);
    
    void post_socket_read(size_t bytes_read);
    
    // Reusable containers of regions for pending socket ops
    buffer_regions_t       _sock_read_regions;
    const_buffer_regions_t _sock_write_regions;
    
    // end index passed to last socket op
    //  (more may have been added since)
    size_t _sock_write_ind;
    
    //  Reusable array of regions to regex-match over
    buffer_regions_t _re_regions;
    
    bool _in_read, _in_write;
    
    buffer_ring _r_ring, _w_ring;
    std::auto_ptr<boost::asio::ip::tcp::socket> _sock;
};

} // end common

#include "common/stream_protocol.impl.hpp"

#endif // end guard

