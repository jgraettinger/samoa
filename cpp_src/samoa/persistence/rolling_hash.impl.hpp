
namespace samoa {
namespace persistence {

template<typename KeyIterator>
const record * rolling_hash::get(
    const KeyIterator & key_begin,
    const KeyIterator & key_end,
    offset_t * rec_ptr_ptr_hint /* = nullptr*/)
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
           std::equal(key_begin, key_end, rec->key_begin()))
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

template<typename KeyIterator>
record * rolling_hash::prepare_record(
    const KeyIterator & key_begin,
    const KeyIterator & key_end,
    unsigned value_length)
{
    size_t key_length = std::distance(key_begin, key_end);

    // bound-check lengths of key & value
    if(key_length >= record::max_key_length)
        throw std::overflow_error("rolling_hash::prepare_record(): "
            "key-size is too large for this table");

    if(value_length >= record::max_value_length)
        throw std::overflow_error("rolling_hash::prepare_record(): "
            "value-size is too large for this table");

    if(!would_fit(key_length, value_length))
        throw std::overflow_error("rolling_hash::prepare_record(): overflow");

    offset_t rec_len = record::allocated_size(key_length, value_length);

    // need to wrap?
    if(_tbl.end + rec_len > _tbl.region_size)
    {
        _tbl.wrap = _tbl.end;
        _tbl.end = records_offset();
    }

    // initialize a new record, beginning at offset _tbl.end
    record * new_rec = (record*)(_region_ptr + _tbl.end);
    new (new_rec) record(key_begin, key_end, value_length);

    return new_rec;
}

template<typename KeyIterator>
bool rolling_hash::mark_for_deletion(
    const KeyIterator & key_begin,
    const KeyIterator & key_end,
    offset_t rec_ptr_ptr /*= 0*/)
{
    size_t key_length = std::distance(key_begin, key_end);

    record * rec = 0;
    if(!rec_ptr_ptr)
    {
        // no hint was provided; do a full lookup
        rec = (record *) get(key_begin, key_end, &rec_ptr_ptr);
    }
    else
    {
        // dereference offset of current record
        offset_t rec_ptr = *(offset_t*)(_region_ptr + rec_ptr_ptr);
        // identify the record pointed to by hint, if any
        rec = rec_ptr ? (record*)(_region_ptr + rec_ptr) : 0;

        if(rec_ptr < records_offset() || rec_ptr >= _tbl.region_size)
        {
            throw std::runtime_error("rolling_hash::mark_for_deletion(): "
                "invalid argument hint (record offset is out of bounds)");
        }

        if(rec && (key_length != rec->key_length() ||
            !std::equal(key_begin, key_end, rec->key_begin())))
        {
            throw std::runtime_error("rolling_hash::mark_for_deletion(): "
                "invalid argument hint (valid record, with wrong key)");
        }
    }

    if(!rec)
        return false;

    // update the previous chain link to point to rec's next,
    //  effectively dropping it from the hash chain
    *(offset_t*)(_region_ptr + rec_ptr_ptr) = rec->next();

    rec->mark_as_dead();
    _tbl.live_record_count -= 1;
    return true;
}

}
}

