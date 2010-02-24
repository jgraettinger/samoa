
// SET MRU cache
//  on set:
//   k => get/update => new v
//   while won't fit:
//    if head() is active:
//      push to get-cache/disk-cache
//    drop_head()
//   set(k,v)
//  on get: get()

// GET MRU cache
//  on get(): get() (optional- if tail 50%, set())
//  on set(): set()

// Disk DB - needs to know expiry
//  on get(): get()
//  on set():
//    do while won't fit (bounded):
//        if head() is active() && exipry() < cur_time:
//            migrate_head()

namespace samoa {

template<int OffsetBytes, 


#define SIZEOF_RECORD (4*4)

inline size_t record::key_length() const
{ return _key_length; }

inline const char * record::key() const
{ return (char*)this + SIZEOF_RECORD; }

inline char * record::key()
{ return (char*)this + SIZEOF_RECORD; }

inline size_t record::value_length() const
{
    size_t t = _val_length[0];
    t = (t << 8) + _val_length[1];
    t = (t << 8) + _val_length[2];
    return t;
}

inline const char * record::value() const
{ return (char*)this + SIZEOF_RECORD + key_length(); }

inline char * record::value()
{ return (char*)this + SIZEOF_RECORD + key_length(); }

inline unsigned int record::expiry() const
{ return _expiry; }


//////////////////////////////////////////////////
///  rolling_hash implementation

rolling_hash::rolling_hash(void * region_begin, size_t region_size, size_t table_size)
 : _cookie(      ((unsigned int*)region_begin)[0]),
   _region_size( ((unsigned int*)region_begin)[1]),
   _table_size(  ((unsigned int*)region_begin)[2]),
   _first(       ((unsigned int*)region_begin)[3]),
   _last(        ((unsigned int*)region_begin)[4]),
   _next(        ((unsigned int*)region_begin)[5]),
   _region_begin( region_begin)
{
    if(region_size < MIN_REGION_SIZE)
        throw std::runtime_error("rolling_hash::rolling_hash(): "
             "region_size < MIN_REGION_SIZE");
    if(region_size > MAX_REGION_SIZE)
        throw std::runtime_error("rolling_hash::rolling_hash(): "
             "region_size > MAX_REGION_SIZE");
    if(table_size * 2 > region_size)
        throw std::runtime_error("rolling_hash::rolling_hash(): "
            "table_size is too large");
    
    if(_cookie == COOKIE_FROZEN)
    {
        // This is an initialized, persisted table
        if(region_size != _region_size)
            throw std::runtime_error("rolling_hash::rolling_hash(): "
                "region already initialized, but stored size != region_size");
        
        _cookie = COOKIE_ACTIVE;
        return;
    }
    
    _cookie      = COOKIE_ACTIVE;
    _region_size = region_size;
    _table_size  = table_size;
    
    // initialize hash index
    memset( index(), 0, sizeof(unsigned int) * _table_size);
    
    _first = _last = _next = (sizeof(unsigned int) * _table_size) + ROLLING_HASH_HEADER_SIZE; 
    return;
}





inline void rolling_hash::mark_dead( record * rec)
{
    if( rec->expiry() == -1)
        throw std::runtime_error("rolling_hash::mark_dead(): "
            "record already dead");
    
    rec->_expiry = -1;
    assert( rec->_prev);
    
    // Remove rec from chain - potential fault point
    *(unsigned int *)(_region_begin + rec->_prev) = _rec->_next;
    return;
}


rolling_hash::

#undef SIZEOF_RECORD


