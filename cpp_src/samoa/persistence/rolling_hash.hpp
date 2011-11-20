#ifndef SAMOA_PERSISTENCE_ROLLING_HASH_HPP
#define SAMOA_PERSISTENCE_ROLLING_HASH_HPP

#include "samoa/persistence/rolling_hash_packet.hpp"
#include <boost/functional/hash.hpp>
#include <stdexcept>

namespace samoa {
namespace persistence {

class rolling_hash
{
public:

    typedef packet::offset_t offset_t;

    typedef rolling_hash_element_proxy element_proxy_t;
    typedef rolling_hash_value_output_stream value_output_stream_t;

    struct value_output_stream;
    struct value_input_stream;

    rolling_hash(void * region_ptr, offset_t region_size, offset_t index_size);

    virtual ~rolling_hash();


    element_proxy_t get(const std::string & key);

    /*
    Preconditions:
     - range(key_begin, key_end) is a potential table key
     - value_length_upper_bound is the maximum number of
       bytes possibly needed to hold the value to-be-written
     - would_fit(distance(key_begin, key_end), value_length_upper_bound)

    Postconditions:
     - a provisional record with the key and a mutable value is returned
     - returned record->value_length() >= value_length_upper_bound
     - the returned record is /not/ yet part of the hash, and any previous
       record for this key is still active

    Rolling back:
     - a second call to prepare_record(), without a call to commit_record(),
       is a semantic rollback of the previously-prepared record
     - the previously-prepared record is invalidated after such a call
    */
    value_output_stream_t put(const std::string & key, unsigned value_length,
        element_proxy_t * hint = 0);

    /*
    Preconditions:
     - range(key_begin, key_end) is a potential table key
     - hint is 0, or was returned by a previous get() for this key

    Postconditions:
     - if key is in hash, the corresponding record is marked for
       deletion and true is returned
     - if key isn't in hash, no changes are made and false is returned
     - no iterators are invalidated; step(marked_record) is still valid

    Notes:
     - optional hint is used to avoid extra lookup to locate
       a record stored under this key
    */
    bool drop(const std::string & key,
        offset_t hint = 0);


    iterator begin() const;
    iterator end() const;




    /*
    Preconditions:
     - head()->is_dead() is true; eg the ring head is marked for deletion

    Postconditions:
     - the ring head becomes the next least-recently-written record
     - memory is reclaimed from the previous head
     - step(previous_head) is no longer valid
    */
    void reclaim_head();

    /*
    Preconditions:
     - head()->is_dead() is false; eg the ring head is a live record
     - new_value_length is 0 (semantically 'use the current length'),
       or is less-than-equal-to the current value_length

    Postconditions:
     - the ring head is rotated to the ring tail
     - step(previous_head) is no longer valid
     - the new ring tail is returned, and it's value may be written into

    rotate_head() can be used to compact the table, by rotating
    live records to the ring tail and uncovering reclaimable records.

    The operation will always succeed, even if would_fit() returns false
     for any key/length value.
    */
    void rotate_head();

    /*
    No Preconditions

    Postconditions:
     - if the hash is empty, nullptr is returned
     - otherwise, the least-recently-written record is returned
    */
    const record * head() const;

    /*
    Preconditions:
     - cur_record is a valid record in the hash

    Postconditions:
     - if cur_record is the tail-most record in the ring, nullptr is returned
     - otherwise, the record written immediately after cur_record is returned
    */
    const record * step(const record * cur) const;

    /*
    Preconditions:
     - key_length/value_length are being considered for ring inclusion

    Postconditions:
     - true is returned if an immediate write would succeed
     - otherwise, false is returned
    */
    bool would_fit(size_t key_length, size_t value_length);

    /*
    Preconditions:
     - 'hint' is a hint returned by a previous get() operation

    Postconditions:
     - if rotating or reclaiming the current head would
       invalidate the hint, true is returned; else false
    */
    bool head_invalidates(offset_t hint) const;

    // metrics

    offset_t total_region_size();
    offset_t used_region_size();

    offset_t total_index_size();
    offset_t used_index_size();

    offset_t total_record_count();
    offset_t live_record_count();

    offset_t dbg_begin()
    { return _tbl.begin; }

    offset_t dbg_end()
    { return _tbl.end; }

    offset_t dbg_wrap()
    { return _tbl.wrap; }

protected:

    unsigned char * _region_ptr;

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
        offset_t total_record_count;
        offset_t live_record_count;

        // offset of first record
        offset_t begin;
        // 1 beyond last record
        offset_t end;
        // if end < begin, 1 beyond final record
        offset_t wrap;
    };

    table_header & _tbl;
};

}
}

#include "samoa/persistence/rolling_hash.impl.hpp"

#endif

