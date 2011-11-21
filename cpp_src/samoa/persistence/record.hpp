#ifndef SAMOA_PERSISTENCE_RECORD_HPP
#define SAMOA_PERSISTENCE_RECORD_HPP

#include <cstddef>
#include <stdexcept>

namespace samoa {
namespace persistence {

class record
{
public:

    typedef unsigned offset_t;

    static const size_t max_key_length = (1 << 11) - 1;
    static const size_t max_value_length = (1 << 27) - 1;

    size_t key_length() const
    { return _meta.key_length; }

    size_t value_length() const
    { return _meta.value_length; }

    bool is_dead() const
    { return _meta.is_dead; }

    bool is_copy() const
    { return _meta.is_copy; }

    const char * key_begin() const
    { return ((char*)this) + header_size(); }

    const char * key_end() const
    { return key_begin() + key_length(); }

    const char * value_begin() const
    { return key_end(); }

    const char * value_end() const
    { return key_end() + value_length(); }

    char * value_begin()
    { return ((char*)this) + header_size() + key_length(); }

    char * value_end()
    { return value_begin() + value_length(); }

    void trim_value_length(size_t value_length)
    {
        if(value_length > _meta.value_length)
        {
            throw std::overflow_error("record::trim_value_length(): "
                "argument value length > this->value_length()");
        }
        _meta.value_length = value_length;
    }

    void mark_as_copy()
    { _meta.is_copy = true; }

    static offset_t allocated_size(size_t key_length, size_t value_length);

private:

    friend class rolling_hash;

    struct {

        // offset of next record in hash chain, or 0
        // Note: order of this field w/in the struct is important,
        //  and relied upon by rolling_hash
        offset_t next;

        // whether this record may be reclaimed
        bool is_dead : 1;

        // whether this record is a cached copy of a record hosted elsewhere
        bool is_copy : 1;

        // length of record key & value
        unsigned key_length : 11;
        unsigned value_length : 27;

        // total size of bit-fields is 5 bytes

    // tell gcc to not word-align (pad) struct bounds
    } __attribute__((__packed__)) _meta;

    template<typename KeyIterator>
    record(const KeyIterator & key_begin, const KeyIterator & key_end,
        unsigned value_length);

    offset_t next() const
    { return _meta.next; }

    void set_next(offset_t next)
    { _meta.next = next; }

    void mark_as_dead()
    { _meta.is_dead = true; }

    // Static methods

    static size_t header_size()
    { return sizeof(_meta); }

};
}
}

#include "samoa/persistence/record.impl.hpp"

#endif
