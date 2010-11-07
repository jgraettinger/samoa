
namespace samoa {

typedef rolling_hash::offset_t offset_t;

// Record to be acted on by a succeeding
//  migrate_head()/drop_head()
inline const rolling_hash::record * rolling_hash::head() const
{
    // empty?
    if(!_tbl.wrap && _tbl.begin == _tbl.end)
        return 0;

    return (record*)(_region_ptr + _tbl.begin);
}

// Given a record, increment to next record in
//  container. Returns 0 if at end.
inline const rolling_hash::record * rolling_hash::step(
    const rolling_hash::record * cur) const
{
    offset_t cur_off = (offset_t)((size_t)cur - (size_t)_region_ptr);
    cur_off += record::allocated_size(
        cur->key_length(), cur->value_length());

    // wrapped?
    if(cur_off == _tbl.wrap)
        cur_off = records_offset();

    // reached end?
    if(cur_off == _tbl.end)
        return 0;

    return (const record*)(_region_ptr + cur_off);
}

template<typename KeyIterator, typename ValIterator>
rolling_hash::record::record(
    const KeyIterator & key_begin,   const KeyIterator & key_end,
    const ValIterator & value_begin, const ValIterator & value_end)
{
    _meta.next = 0;
    _meta.is_dead = false;
    _meta.key_length = std::distance(key_begin, key_end);
    _meta.value_length = std::distance(value_begin, value_end);

    std::copy(key_begin, key_end, (char*)key());
    std::copy(value_begin, value_end, (char*)value());
}

inline offset_t rolling_hash::record::allocated_size(
    size_t key_length, size_t value_length)
{
    offset_t rec_len = header_size() + key_length + value_length;

    // records are aligned to sizeof(offset_t)
    if(rec_len % sizeof(offset_t))
        rec_len += sizeof(offset_t) - (rec_len % sizeof(offset_t));

    return rec_len;
}

template<typename KeyIterator>
const rolling_hash::record * rolling_hash::get(
    // Input: key sequence
    const KeyIterator & key_begin,
    const KeyIterator & key_end,
    // Output: matching record (null if not found)
    offset_t * rec_ptr_ptr_hint /* = 0*/)
{
    size_t key_length = std::distance(key_begin, key_end);
    size_t hash_val = boost::hash_range(key_begin, key_end);

    // hash the key to index bucket, and initialize a double-
    //  indirection (offset to the offset of the record)
    offset_t rec_ptr_ptr = index_offset() + \
        (hash_val % _tbl.index_size) * sizeof(offset_t);

    // dereference to offset of record
    offset_t rec_ptr = *(offset_t*)(_region_ptr + rec_ptr_ptr);

    while(rec_ptr != 0)
    {
        // dereference to record
        record * rec = (record*)(_region_ptr + rec_ptr);

        // key match?
        if(key_length == rec->key_length() &&
           std::equal(key_begin, key_end, rec->key()))
        {
           break;
        }

        // otherwise, follow the chain

        // record.next happens to be the first bytes of record
        rec_ptr_ptr = rec_ptr;
        rec_ptr = *(offset_t*)(_region_ptr + rec_ptr_ptr);
    }

    // optionally return the offset of the offset to the record as
    //  a hint for subsequent operations which update the hash chain
    if(rec_ptr_ptr_hint)
        *rec_ptr_ptr_hint = rec_ptr_ptr;

    if(rec_ptr)
        return (const record*)(_region_ptr + rec_ptr);
    else
        return 0;
}

// Queries whether sufficient space is available for
//  an immediate write of a key/value record
inline bool rolling_hash::would_fit(size_t key_length, size_t value_length)
{
    offset_t rec_len = record::allocated_size(key_length, value_length);

    // would cause a wrap?
    if(_tbl.end + rec_len > _tbl.region_size)
        return records_offset() + rec_len <= _tbl.begin;

    if(_tbl.wrap && _tbl.end + rec_len > _tbl.begin)
        return false;

    return true;
}

// Adds a new record for this key/value.
// A prior entry under key is marked for collection.
// Throws on too little space, or key/value overflow
template<typename KeyIterator, typename ValIterator>
void rolling_hash::set(
    // key sequence
    const KeyIterator & key_begin,
    const KeyIterator & key_end,
    // value sequence 
    const ValIterator & val_begin,
    const ValIterator & val_end,
    // hint returned by previous get()
    offset_t rec_ptr_ptr /*= 0*/)
{
    size_t key_length = std::distance(key_begin, key_end);
    size_t value_length = std::distance(val_begin, val_end);

    // bound-check lengths of key & value
    if(key_length >= record::max_key_length)
        throw std::overflow_error("rolling_hash::set(): "
            "key-size is too large for this table");

    if(value_length >= record::max_value_length)
        throw std::overflow_error("rolling_hash::set(): "
            "value-size is too large for this table");

    if(!would_fit(key_length, value_length))
        throw std::overflow_error("rolling_hash::set(): "
            "table overflow -- drop some keys");

    offset_t rec_len = record::allocated_size(key_length, value_length);

    // need to wrap?
    if(_tbl.end + rec_len > _tbl.region_size)
    {
        _tbl.wrap = _tbl.end;
        _tbl.end = records_offset();
    }

    // initialize a new record, beginning at offset _tbl.end
    record * new_rec = (record*)(_region_ptr + _tbl.end);
    new (new_rec) record(key_begin, key_end, val_begin, val_end);

    // identify the pre-existing record, if there is one
    //  if rec_ptr_ptr wasn't provided, look it up--we'll
    //  need it to insert the new record into the chain

    record * old_rec = 0;
    if(!rec_ptr_ptr)
    {
        // no hint was provided; do a full lookup
        old_rec = (record *) get(key_begin, key_end, &rec_ptr_ptr);
    }
    else
    {
        // dereference offset of current record
        offset_t old_rec_ptr = *(offset_t*)(_region_ptr + rec_ptr_ptr);
        // identify the record pointed to by hint, if any
        old_rec = old_rec_ptr ? (record*)(_region_ptr + old_rec_ptr) : 0;
    }

    if(old_rec)
    {
        assert(key_length == rec->key_length() &&
            std::equal(key_begin, key_end, rec->key()));

        // swap the old record for the new within the hash chain
        new_rec->set_next(old_rec->next());

        // mark old record for collection
        old_rec->set_dead();
    }
    else
    {
        _tbl.record_count += 1;
    }

    // update the previous link in the hash chain to point to new_rec
    // the pointer we're updating here may be the next field of another
    // record, or it may be a location within the table index
    *(offset_t*)(_region_ptr + rec_ptr_ptr) = _tbl.end;

    // update ring to reflect allocation of new_rec
    _tbl.end += rec_len;
}

// Drops a matching record. Returns whether a record was found & dropped
template<typename KeyIterator>
bool rolling_hash::drop(
    const KeyIterator & key_begin, const KeyIterator & key_end)
{
    offset_t rec_ptr_ptr;
    record * rec = (record *) get(key_begin, key_end, &rec_ptr_ptr);

    if(!rec)
        return false;

    // update the previous chain link to point to rec's next,
    //  effectively dropping it from the hash chain
    *(offset_t*)(_region_ptr + rec_ptr_ptr) = rec->next();

    rec->set_dead();
    return true;
}

inline void rolling_hash::reclaim_head()
{
    record * rec = (record*)(_region_ptr + _tbl.begin);
    offset_t rec_len = record::allocated_size(
        rec->key_length(), rec->value_length());

    _tbl.begin += rec_len;
    if(_tbl.begin == _tbl.wrap)
    {
        _tbl.wrap = 0;
        _tbl.begin = records_offset();
    }
}

};

