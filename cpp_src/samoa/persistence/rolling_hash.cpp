
#include "samoa/persistence/rolling_hash.hpp"
#include <string.h>

namespace samoa {
namespace persistence {

typedef rolling_hash::offset_t offset_t;

rolling_hash::rolling_hash(
    void * region_ptr, offset_t region_size, offset_t index_size)
 : _region_ptr((unsigned char *) region_ptr),
   _tbl(*(table_header*) region_ptr)
{
    if(region_size < (sizeof(table_header) + index_size * sizeof(offset_t)))
        throw std::runtime_error("rolling_hash::rolling_hash(): "
            "region_size too small");

    if(_tbl.state == FROZEN)
    {
        // This is an initialized, persisted table;
        //  do a few integrity checks

        if(_tbl.offset_byte_size != sizeof(offset_t))
            throw std::runtime_error("rolling_hash::rolling_hash(): "
                "stored table uses a different offset size");

        if(_tbl.region_size != region_size)
            throw std::runtime_error("rolling_hash::rolling_hash(): "
                "stored region_size != region_size");
    }
    else
    {
        _tbl.offset_byte_size = sizeof(offset_t);
        _tbl.region_size = region_size;
        _tbl.index_size = index_size;
        _tbl.total_record_count = 0;
        _tbl.live_record_count = 0;
        _tbl.begin = _tbl.end = records_offset();
        _tbl.wrap = 0;

        // zero the hash index
        memset(_region_ptr + index_offset(),
            0, _tbl.index_size * sizeof(offset_t));
    }

    _tbl.state = ACTIVE;
    return;
}

rolling_hash::~rolling_hash()
{ }

void rolling_hash::commit_record(offset_t rec_ptr_ptr /*= 0*/)
{
    record * new_rec = (record*)(_region_ptr + _tbl.end);

    offset_t rec_len = record::allocated_size(
        new_rec->key_length(), new_rec->value_length());

    // identify the pre-existing record, if there is one
    //  if rec_ptr_ptr wasn't provided, look it up--we'll
    //  need it to insert the new record into the chain

    record * old_rec = 0;
    if(!rec_ptr_ptr)
    {
        // no hint was provided; do a full lookup
        old_rec = (record *) get(
            new_rec->key_begin(), new_rec->key_end(), &rec_ptr_ptr);
    }
    else
    {
        // dereference offset of current record
        offset_t old_rec_ptr = *(offset_t*)(_region_ptr + rec_ptr_ptr);
        // identify the record pointed to by hint, if any
        old_rec = old_rec_ptr ? (record*)(_region_ptr + old_rec_ptr) : 0;

        if(old_rec && (new_rec->key_length() != old_rec->key_length() ||
            !std::equal(new_rec->key_begin(), new_rec->key_end(), old_rec->key_begin())))
        {
            throw std::runtime_error("rolling_hash::commit_record(): "
                "invalid argument hint");
        }
    }

    if(old_rec)
    {
        // swap the old record for the new within the hash chain
        new_rec->set_next(old_rec->next());
        // mark old record for deletion
        old_rec->mark_as_dead();
    }
    else
    {
        _tbl.live_record_count += 1;
    }

    // update the previous link in the hash chain to point to new_rec
    // the pointer we're updating here may be the next field of another
    // record, or it may be a location within the table index
    *(offset_t*)(_region_ptr + rec_ptr_ptr) = _tbl.end;

    // update ring to reflect allocation of new_rec
    _tbl.end += rec_len;

    _tbl.total_record_count += 1;
}

void rolling_hash::reclaim_head()
{
    // empty?
    if(!_tbl.wrap && _tbl.begin == _tbl.end)
        throw std::underflow_error("rolling_hash::reclaim_head(): empty");

    record * rec = (record*)(_region_ptr + _tbl.begin);
    offset_t rec_len = record::allocated_size(
        rec->key_length(), rec->value_length());

    if(!rec->is_dead())
    {
        throw std::runtime_error("rolling_hash::reclaim_head(): "
            "head is not marked for deletion");
    }

    _tbl.begin += rec_len;
    if(_tbl.begin == _tbl.wrap)
    {
        _tbl.wrap = 0;
        _tbl.begin = records_offset();
    }
    _tbl.total_record_count -= 1;
}

void rolling_hash::rotate_head()
{
    // empty?
    if(!_tbl.wrap && _tbl.begin == _tbl.end)
        throw std::underflow_error("rolling_hash::rotate_head(): empty");

    record * rec = (record*)(_region_ptr + _tbl.begin);

    if(rec->is_dead())
    {
        throw std::runtime_error("rolling_hash::rotate_head(): "
            "head is marked for deletion");
    }

    // locate record within hash-chain
    offset_t rec_ptr_ptr;
    get(rec->key_begin(), rec->key_end(), &rec_ptr_ptr);

    assert(rec_ptr_ptr);

    // rotate ring indices, dropping head & immediately re-allocating it
    offset_t rec_begin = _tbl.begin;
    offset_t rec_len = record::allocated_size(
        rec->key_length(), rec->value_length());

    // rotate ring indices, dropping head
    _tbl.begin += rec_len;
    if(_tbl.begin == _tbl.wrap)
    {
        _tbl.wrap = 0;
        _tbl.begin = records_offset();
    }

    // re-allocate it at ring tail
    if(_tbl.end + rec_len > _tbl.region_size)
    {
        // need to wrap
        _tbl.wrap = _tbl.end;
        _tbl.end = records_offset();
    }

    // copy raw bytes of record from old to new location
    // Big Fat Note: we're quite possibly overwriting the old record as
    // we write the new one. This is only safely done by copying-forward
    std::copy(
        _region_ptr + rec_begin,
        _region_ptr + rec_begin + rec_len,
        _region_ptr + _tbl.end);

    // update the previous link in the hash chain to point to new_rec
    // the pointer we're updating here may be the next field of another
    // record, or it may be a location within the table index
    *(offset_t*)(_region_ptr + rec_ptr_ptr) = _tbl.end;

    _tbl.end += rec_len;
}


const record * rolling_hash::head() const
{
    // empty?
    if(!_tbl.wrap && _tbl.begin == _tbl.end)
        return 0;

    return (record*)(_region_ptr + _tbl.begin);
}

const record * rolling_hash::step(const record * cur) const
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

bool rolling_hash::would_fit(size_t key_length, size_t value_length)
{ return would_fit(record::allocated_size(key_length, value_length)); }

bool rolling_hash::would_fit(size_t record_length)
{
    // would cause a wrap?
    if(_tbl.end + record_length > _tbl.region_size)
        return records_offset() + record_length <= _tbl.begin;

    if(_tbl.wrap && _tbl.end + record_length > _tbl.begin)
        return false;

    return true;
}

offset_t rolling_hash::total_region_size()
{ return _tbl.region_size; }

offset_t rolling_hash::used_region_size()
{
    size_t used = records_offset();

    if(_tbl.end < _tbl.begin)
    {
        used += (_tbl.wrap - _tbl.begin);
        used += (_tbl.end - records_offset());
    }
    else
        used += _tbl.end - _tbl.begin;

    return used;
}

offset_t rolling_hash::total_index_size()
{ return _tbl.index_size; }

offset_t rolling_hash::used_index_size()
{
    size_t used = 0;

    offset_t * index = (offset_t *)(_region_ptr + index_offset());
    for(size_t i = 0; i != _tbl.index_size; ++i)
        used += index[i] ? 1 : 0;

    return used;
}

offset_t rolling_hash::total_record_count()
{ return _tbl.total_record_count; }

offset_t rolling_hash::live_record_count()
{ return _tbl.live_record_count; }

}
}

