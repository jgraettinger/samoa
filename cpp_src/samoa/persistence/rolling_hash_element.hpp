#ifndef SAMOA_PERSISTENCE_ROLLING_HASH_ELEMENT_PROXY_HPP
#define SAMOA_PERSISTENCE_ROLLING_HASH_ELEMENT_PROXY_HPP

#include "samoa/persistence/rolling_hash_packet.hpp"
#include "samoa/error.hpp"


namespace samoa {
namespace persistence {

class rolling_hash_key_gather_iterator :
    public boost::iterator_facade<
        rolling_hash_key_iterator, const char, boost::forward_traversal_tag>
{
public:

    rolling_hash_key_gather_iterator()
     :  _hash(nullptr),
        _next(nullptr),
        _cur(nullptr),
        _end(nullptr)
    { }

    rolling_hash_key_gather_iterator(rolling_hash * hash,
        rolling_hash_packet * packet)
     :  _hash(hash),
        _next(packet),
        _cur(nullptr),
        _end(nullptr)
    {
        increment();
    }

private:

    friend class boost::iterator_core_access;

    void increment()
    {
        if(++_cur >= _end)
        {
            if(_next && _next->key_length())
            {
                // update cur & end w/ ranges from next packet
                _cur = _next->key_begin();
                _end = _next->key_end();

                if(!_next->completes_sequence())
                {
                    _next = _hash->next_packet(_next);
                    SAMOA_ASSERT(_next->continues_sequence());
                }
                else
                {
                    _next = nullptr;
                }
            }
            else
            {
                _next = _cur = _end = nullptr;
            }
        }
    }

    bool equal(const rolling_hash_key_gather_iterator & other) const
    { return _cur == other._cur; }

    char dereference() const
    { return *_cur; }

    rolling_hash * _hash;
    rolling_hash_packet * _packet;
    const char * _cur, _end; 
};




class rolling_hash_element_proxy
{
public:

    typedef rolling_hash_packet packet_t;
    typedef rolling_hash_value_input_stream value_input_stream_t;

    bool is_null() const
    { return !_packet; }

    


    int compare_key(const std::string & other_key) const;

    value_input_stream_t get_value() const;

private:

    friend class rolling_hash;

    rolling_hash_element_proxy(rolling_hash * hash, packet_t * packet);

    rolling_hash * _hash;
    packet_t * _packet;
    packet_t::offset_t _packet_pointer;
};

}
}

#endif

