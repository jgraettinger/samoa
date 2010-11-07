
#include "samoa/rolling_hash.hpp"
#include <boost/python.hpp>

namespace samoa {

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
        _tbl.begin = _tbl.end = records_offset();
        _tbl.wrap = 0;

        // zero the hash index
        memset(_region_ptr + index_offset(),
            0, _tbl.index_size * sizeof(offset_t));
    }

    _tbl.state = ACTIVE;
    return;
}

bool rolling_hash::migrate_head()
{
    // empty?
    if(!_tbl.wrap && _tbl.begin == _tbl.end)
        return true;

    record * old_rec = (record*)(_region_ptr + _tbl.begin);

    if(old_rec->is_dead())
    {
        reclaim_head();
        return true;
    }

    // this is a live record; need to do the work of migrating it

    size_t key_length = old_rec->key_length();
    size_t value_length = old_rec->value_length();

    if(!would_fit(key_length, value_length))
        return false;

    size_t rec_len = record::allocated_size(key_length, value_length);

    // need to wrap?
    if(_tbl.end + rec_len > _tbl.region_size)
    {
        _tbl.wrap = _tbl.end;
        _tbl.end = records_offset();
    }

    // obtain the location of this record within the hash chain
    offset_t rec_ptr_ptr;
    get(old_rec->key(), old_rec->key() + key_length, &rec_ptr_ptr);

    // initialize a copy of the record, beginning at offset _tbl.end
    record * new_rec = (record*)(_region_ptr + _tbl.end);
    new (new_rec) record(old_rec->key(), old_rec->key() + key_length,
        old_rec->value(), old_rec->value() + value_length);

    // update the hash chain; new_rec captures the tail...
    new_rec->set_next(old_rec->next());
    // and update the previous chain link to point to new_rec
    *(offset_t*)(_region_ptr + rec_ptr_ptr) = _tbl.end;

    // update ring to reflect allocation
    _tbl.end += rec_len;

    // reclaim old record
    reclaim_head();
    return true;
}

void rolling_hash::drop_head()
{
    // empty?
    if(!_tbl.wrap && _tbl.begin == _tbl.end)
        return;

    record * rec = (record*)(_region_ptr + _tbl.begin);

    if(!rec->is_dead())
    {
        // obtain the location of this record within it's hash chain
        offset_t rec_ptr_ptr;
        get(rec->key(), rec->key() + rec->key_length(), &rec_ptr_ptr);

        // update the previous chain link to point to rec's next,
        //  effectively dropping it from the hash chain
        *(offset_t*)(_region_ptr + rec_ptr_ptr) = rec->next();
    }

    // reclaim record
    reclaim_head();

    _tbl.record_count -= 1;
    return;
}

size_t rolling_hash::total_region_size()
{ return _tbl.region_size; }

size_t rolling_hash::used_region_size()
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

size_t rolling_hash::total_index_size()
{ return _tbl.index_size; }

size_t rolling_hash::used_index_size()
{
    size_t used = 0;

    offset_t * index = (offset_t *)(_region_ptr + index_offset());
    for(size_t i = 0; i != _tbl.index_size; ++i)
        used += index[i] ? 1 : 0;

    return used;
}

// Python Bindings

namespace bpl = boost::python;

boost::python::object none_obj( 
    ((struct boost::python::detail::borrowed_reference_t *) ((void*) Py_None)));

struct py_rolling_hash_iter
{
    rolling_hash * hash;
    const rolling_hash::record * cur;

    bpl::tuple next()
    {
        while(cur && cur->is_dead())
            cur = hash->step(cur);

        if(!cur)
        {
            PyErr_SetObject(PyExc_StopIteration, Py_None);
            boost::python::throw_error_already_set();
        }

        bpl::tuple ret = bpl::make_tuple(
            bpl::str(cur->key(), cur->key() + cur->key_length()),
            bpl::str(cur->value(), cur->value() + cur->value_length()));

        cur = hash->step(cur);

        return ret;
    }
};

py_rolling_hash_iter py_iter(rolling_hash * hash)
{
    py_rolling_hash_iter iter;
    iter.hash = hash;
    iter.cur = hash->head();
    return iter;
}

bpl::object py_get(rolling_hash * hash, const bpl::str & key)
{
    const char * key_begin = PyString_AsString(key.ptr());
    const char * key_end = key_begin + PyString_GET_SIZE(key.ptr());

    const rolling_hash::record * rec = hash->get(key_begin, key_end);
    if(rec)
        return bpl::str(rec->value(), rec->value() + rec->value_length());
    else
        return none_obj;
}

void py_set(rolling_hash * hash, const bpl::str & key, const bpl::str & val)
{
    const char * key_begin = PyString_AsString(key.ptr());
    const char * key_end = key_begin + PyString_GET_SIZE(key.ptr());

    const char * val_begin = PyString_AsString(val.ptr());
    const char * val_end = val_begin + PyString_GET_SIZE(val.ptr());

    hash->set(key_begin, key_end, val_begin, val_end);
}

bool py_drop(rolling_hash * hash, const bpl::str & key)
{
    const char * key_begin = PyString_AsString(key.ptr());
    const char * key_end = key_begin + PyString_GET_SIZE(key.ptr());

    return hash->drop(key_begin, key_end);
}

bpl::object py_head(rolling_hash * hash)
{
    const rolling_hash::record * head = hash->head();

    if(head && !head->is_dead())
        return bpl::make_tuple(
            bpl::str(head->key(), head->key() + head->key_length()),
            bpl::str(head->value(), head->value() + head->value_length()));

    return none_obj;
}

void make_rolling_hash_bindings()
{
    bpl::class_<py_rolling_hash_iter>("__RollingHash_iterator", bpl::no_init)
        .def("next", &py_rolling_hash_iter::next);

    bpl::class_<rolling_hash, boost::noncopyable>("RollingHash", bpl::no_init)
        .def("__iter__", &py_iter)
        .def("get", &py_get)
        .def("would_fit", &rolling_hash::would_fit)
        .def("set", &py_set)
        .def("drop", &py_drop)
        .def("head", &py_head)
        .def("migrate_head", &rolling_hash::migrate_head)
        .def("drop_head", &rolling_hash::drop_head)
        .add_property("total_region_size", &rolling_hash::total_region_size)
        .add_property("used_region_size", &rolling_hash::used_region_size)
        .add_property("total_index_size", &rolling_hash::total_index_size)
        .add_property("used_index_size", &rolling_hash::used_index_size);
}

};

