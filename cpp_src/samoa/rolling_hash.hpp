#ifndef SAMOA_ROLLING_HASH_HPP
#define SAMOA_ROLLING_HASH_HPP

#include <boost/functional/hash.hpp>
#include <stdexcept>

namespace samoa {

class rolling_hash
{
public:

    typedef unsigned offset_t;

    class record
    {
    public:

        static const size_t max_key_length = (1 << 12) - 1;
        static const size_t max_value_length = (1 << 27) - 1;

        size_t key_length() const
        { return _meta.key_length; }

        size_t value_length() const
        { return _meta.value_length; }

        bool is_dead() const
        { return _meta.is_dead; }

        const char * key() const
        { return ((char*)this) + header_size(); }

        const char * value() const
        { return ((char*)this) + header_size() + key_length(); }

    private:

        friend class rolling_hash;

        struct {

            // offset of next record in hash chain, or 0
            offset_t next;

            // whether this record may be reclaimed
            bool is_dead : 1;

            // length of record key & value
            unsigned key_length : 12;
            unsigned value_length : 27;

            // total size of bit-fields is 5 bytes

        // tell gcc to not word-align (pad) struct bounds
        } __attribute__((__packed__)) _meta;

        template<typename KeyIterator, typename ValIterator>
        record(const KeyIterator & key_begin,   const KeyIterator & key_end,
               const ValIterator & value_begin, const ValIterator & value_end);

        offset_t next() const
        { return _meta.next; }

        void set_next(offset_t next)
        { _meta.next = next; }

        void set_dead()
        { _meta.is_dead = true; }

        // Static methods

        static size_t header_size()
        { return sizeof(_meta); }

        static offset_t allocated_size(size_t key_length, size_t value_length);
    };

    rolling_hash(void * region_ptr, offset_t region_size, offset_t index_size);

    virtual ~rolling_hash()
    { }

    // Attempts to find a record matching the key
    //  within the table. Returns the matching
    //  record (or null if not found). Optionally returns
    //  and an insertion/update hint by argument reference.
    template<typename KeyIterator>
    const record * get(
        // Input: key sequence
        const KeyIterator & key_begin,
        const KeyIterator & key_end,
        // Output: matching record (null if not found)
        offset_t * hint = 0);

    // Queries whether sufficient space is available for
    //  an immediate write of a key/value record
    bool would_fit(size_t key_length, size_t value_length);

    // Adds a new record for this key/value.
    // A prior entry under key is marked for collection.
    // Throws on too little space, or key/value overflow
    template<typename KeyIterator, typename ValIterator>
    void set(
        // key sequence
        const KeyIterator & key_begin,
        const KeyIterator & key_end,
        // value sequence 
        const ValIterator & val_begin,
        const ValIterator & val_end,
        // hint returned by previous get()
        offset_t hint = 0);

    // Drops a matching record. Returns whether a record was found & dropped
    template<typename KeyIterator>
    bool drop(const KeyIterator & key_begin, const KeyIterator & key_end);

    // Record to be acted on by a succeeding
    //  migrate_head()/drop_head()
    const record * head() const;

    // Given a record, increment to next record in
    //  container. Returns 0 if at end.
    const record * step(const record * cur) const;

    // Least recently added/moved record is moved to the tail of the ring,
    //   or is dropped if marked as collectable. buffer-ring head steps.
    //  Returns false if op would overflow
    bool migrate_head();

    // least recently added/moved record
    //  is dropped. buffer-ring head steps.
    void drop_head();

    // metrics

    size_t total_region_size();
    size_t used_region_size();

    size_t total_index_size();
    size_t used_index_size();

protected:

    unsigned char * _region_ptr;

    inline void reclaim_head();

    enum table_state_enum {
        FROZEN = 0xf0f0f0f0,
        ACTIVE = FROZEN + 1
    };

    offset_t index_offset() const
    { return sizeof(table_header); }

    offset_t records_offset() const
    { return sizeof(table_header) + _tbl.index_size * sizeof(offset_t); }

    struct table_header {

        unsigned state;
        unsigned offset_byte_size;
        offset_t region_size;
        offset_t index_size;
        offset_t record_count;

        // offset of first record
        offset_t begin;
        // 1 beyond last record
        offset_t end;
        // if end < begin, 1 beyond final record
        offset_t wrap;
    };

    table_header & _tbl;
};

}; // end namespace samoa

#include "samoa/rolling_hash.impl.hpp"

#endif

