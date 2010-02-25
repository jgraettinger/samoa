#ifndef COMMON_ROLLING_HASH_HPP
#define COMMON_ROLLING_HASH_HPP

#include <boost/functional/hash.hpp>
#include <stdexcept>
#include <iostream>

#define __RH_EXTRACT(t, size_array, n_bytes) \
    {\
        t = 0;\
        for(size_t i = 0; i != n_bytes; ++i)\
            t = (t<<8) + (size_array)[i];\
    }

#define __RH_SET(new_size, size_array, n_bytes) \
    {\
        size_t t = new_size;\
        for(size_t i = 1; i <= n_bytes; ++i)\
        {\
            (size_array)[n_bytes - i] = t & 0xff;\
            t = (t>>8);\
        }\
    }

namespace common {

using namespace std;

enum HashState {
    FROZEN = 0xf0f0f0f0,
    ACTIVE = FROZEN + 1
};

template<
    int OffsetBytes,
    int KeyLenBytes,
    int ValLenBytes,
    int ExpireBytes
>
class rolling_hash
{
public:
    
    static const int offset_bytes = OffsetBytes;
    static const int key_len_bytes = KeyLenBytes;
    static const int val_len_bytes = ValLenBytes;
    static const int expire_bytes = ExpireBytes;
    
    class record
    {
    public:
        
        size_t key_length() const
        { size_t t = 0; __RH_EXTRACT(t, _key_length, KeyLenBytes); return t; }
        
        size_t value_length() const
        { size_t t = 0; __RH_EXTRACT(t, _val_length, ValLenBytes); return t; }
        
        unsigned int expiry() const
        { size_t t = 0; __RH_EXTRACT(t, _expiry, ExpireBytes); return t; }
        
        bool is_dead() const
        { return expiry() == 1; }
        
        const char * key() const
        { return ((char*)this) + header_size(); }
        
        const char * value() const
        { return ((char*)this) + header_size() + key_length(); }
        
    private:
        
        size_t next() const
        { size_t t = 0; __RH_EXTRACT(t, _next, OffsetBytes); return t; }
        
        size_t prev() const
        { size_t t = 0; __RH_EXTRACT(t, _prev, OffsetBytes); return t; }
        
        void set_next(size_t next)
        { __RH_SET(next, _next, OffsetBytes); }
        
        void set_prev(size_t prev)
        { __RH_SET(prev, _prev, OffsetBytes); }
        
        void set_expiry(size_t expiry)
        { __RH_SET(expiry, _expiry, ExpireBytes); }
        
        static size_t header_size()
        {
            return ExpireBytes + KeyLenBytes + \
                ValLenBytes + (OffsetBytes * 2);
        }
        
        template<typename KeyIterator, typename ValIterator>
        record(
            const KeyIterator & key_begin, const KeyIterator & key_end,
            const ValIterator & val_begin, const ValIterator & val_end,
            size_t expiry)
        {
            __RH_SET(expiry, _expiry, ExpireBytes);
            __RH_SET(std::distance(key_begin, key_end), _key_length, KeyLenBytes);
            __RH_SET(std::distance(val_begin, val_end), _val_length, ValLenBytes);
            memset(_next, 0, OffsetBytes);
            memset(_prev, 0, OffsetBytes);
            std::copy(key_begin, key_end, (char*)key());
            std::copy(val_begin, val_end, (char*)value());
        }
        
        // record is not word-aligned in rolling-hash,
        //  thus expiry must be stored as char array
        // 0 iff no expiry, 1 iff 'zombie', or expiry
        unsigned char _expiry[ExpireBytes];
        
        unsigned char _key_length[KeyLenBytes];
        unsigned char _val_length[ValLenBytes];
        
        // 0, or offset of next record in chain
        unsigned char _next[OffsetBytes];
        // 0, or offset of prev record's _next offset
        unsigned char _prev[OffsetBytes];
        
        friend class rolling_hash;
    };
    
    
    rolling_hash(void * region_ptr, size_t region_size, size_t table_size)
     : _region_ptr((unsigned char *) region_ptr),
       _tbl(*(table_header*) region_ptr)
    {
        if(region_size >= (1LLU << (8 * OffsetBytes)))
            throw std::runtime_error("rolling_hash::rolling_hash(): "
                "region_size too large");
        if(region_size < (sizeof(table_header) + table_size * OffsetBytes))
            throw std::runtime_error("rolling_hash::rolling_hash(): "
                "region_size too small");
        
        if(_tbl.state == FROZEN)
        {
            std::cerr << "found FROZEN table" << std::endl;


            // This is an initialized, persisted table
            if(_tbl.region_size != region_size)
                throw std::runtime_error("rolling_hash::rolling_hash(): "
                    "stored region_size != region_size");
            _tbl.state = ACTIVE;
            return;
        }
        else
            std::cerr << "NEW table " << _tbl.state << std::endl;

        _tbl.state = ACTIVE;
        _tbl.region_size = region_size;
        _tbl.table_size = table_size;
        
        // initialize the hash index
        memset(_region_ptr + index_offset(), 0, _tbl.table_size * OffsetBytes);
        _tbl.begin = _tbl.end = records_offset();
        _tbl.wrap = 0;
        return;
    }
    
    virtual ~rolling_hash()
    { }
    
    void freeze()
    { _tbl.state = FROZEN; }
    
    // Attempts to find a record matching the key
    //  within the table. Returns the matching
    //  record if found, else Null. Also returned
    //  is an insertion/update hint.
    template<typename KeyIterator>
    void get(
        // Input: key sequence
        const KeyIterator & key_begin,
        const KeyIterator & key_end,
        // Output: matching record (only if found)
        record * & rec,
        // Output: insertion hint (offset to offset to record)
        size_t & hint)
    {
        size_t key_length = std::distance(key_begin, key_end);
        size_t hash_val = boost::hash_range(key_begin, key_end);
        
        // hash to initial offset of offset of record
        hint = index_offset() + \
            (hash_val % _tbl.table_size) * OffsetBytes;
        
        // dereference offset of record/record
        size_t rec_off;
        __RH_EXTRACT(rec_off, _region_ptr + hint, OffsetBytes);
        rec = (record*)(_region_ptr + rec_off);
        
        while(rec_off != 0)
        {
            // key match?
            if( key_length == rec->key_length() &&
                std::equal(key_begin, key_end, rec->key()))
                break;
            
            // follow chain
            __RH_EXTRACT(rec_off, rec->_next, OffsetBytes);
            hint = (size_t)&rec->_next - (size_t)_region_ptr;
            rec = (record*)(_region_ptr + rec_off);
        }
        // if not found, return Null
        if(rec_off == 0)
            rec = 0;
        return;
    }
    
    // Queries whether sufficient space is available for
    //  an immediate write of a key/value record
    bool would_fit(size_t key_length, size_t val_length)
    {
        // length overflow
        if(key_length >= (1L << (8 * KeyLenBytes)))
            return false;
        if(val_length >= (1L << (8 * ValLenBytes)))
            return false;
        
        size_t rec_len = key_length + val_length + record::header_size();
        
        // would cause a wrap?
        if(_tbl.end + rec_len > _tbl.region_size)
            return records_offset() + rec_len <= _tbl.begin;
        
        if(_tbl.wrap && _tbl.end + rec_len > _tbl.begin)
            return false;
        
        return true;
    }
    
    // Adds a new record for this key/value.
    // A prior entry under key is set as inactive.
    // Throws on to little space, or key/value overflow
    template<typename KeyIterator, typename ValIterator>
    void set(
        // key sequence
        const KeyIterator & key_begin,
        const KeyIterator & key_end,
        // value sequence 
        const ValIterator & val_begin,
        const ValIterator & val_end,
        // optional expiry (0 => no expiry)
        unsigned int expiry = 0,
        // hint returned by previous get();
        //  offset of offset of record
        size_t hint = 0
    )
    {
        size_t key_length = std::distance(key_begin, key_end);
        size_t val_length = std::distance(val_begin, val_end);
        
        if(!would_fit(key_length, val_length))
            throw std::overflow_error("rolling_hash::set(): "
                "key/value too large");
        
        size_t rec_len = key_length + val_length + record::header_size();
        
        // need to wrap?
        if(_tbl.end + rec_len > _tbl.region_size)
        {
            _tbl.wrap = _tbl.end;
            _tbl.end = records_offset();
        }
        
        record * rec;
        if(!hint)
            get(key_begin, key_end, rec, hint);
        else
        {
            // identify the record pointed to by hint, if any
            size_t rec_off = 0;
            __RH_EXTRACT(rec_off, _region_ptr + hint, OffsetBytes);
            rec = rec_off ? (record*)(_region_ptr + rec_off) : 0;
        }
        
        // fill new record
        record * new_rec = (record*)(_region_ptr + _tbl.end);
        new (new_rec) record(key_begin, key_end, val_begin, val_end, expiry);
        
        // update chain
        if(rec)
        {
            assert( key_length == rec->key_length() &&
                std::equal(key_begin, key_end, rec->key()));
            
            size_t next = rec->next();
            // rec->next => new_rec->next
            new_rec->set_next( next);
            // rec->next->prev => new_rec
            if(next)
            {
                ((record*)(_region_ptr + next))->set_prev(
                    ((size_t)&new_rec->_next - (size_t)_region_ptr));
            }
            
            // mark rec as zombie
            rec->set_expiry(1);
        }
        else
            _tbl.record_count += 1;
        
        // new_rec => new_rec->prev->next
        __RH_SET(_tbl.end, _region_ptr + hint, OffsetBytes);
        // hint => new_rec->prev
        new_rec->set_prev(hint);
        
        // update ring to reflect allocation
        _tbl.end += rec_len;
    }
    
    // Record to be acted on by a succeeding
    //  migrate_head()/drop_head()
    // Null if current head is inactive.
    // Allows multi-tier MRU to detect when
    //  an active record is about to 'fall off',
    const record * head() const
    {
        // empty?
        if(!_tbl.wrap && _tbl.begin == _tbl.end)
            return 0;
        
        return (record*)(_region_ptr + _tbl.begin);
    }
    
    // Given a record, incremente to next record in
    //  container. Returns 0 if at end.
    const record * step(const record * cur) const
    {
        size_t cur_off = (unsigned char*)cur - _region_ptr;
        cur_off += record::header_size() + cur->key_length() + cur->value_length();
        
        // wrapped?
        if(cur_off == _tbl.wrap)
            cur_off = records_offset();
        
        // reached end?
        if(cur_off == _tbl.end)
            return 0;
        
        return (const record*)(_region_ptr + cur_off);
    }
    
    // least recently added/moved record is moved to the tail of the ring,
    //  if active. buffer-ring head steps. Returns false if op would overflow
    bool migrate_head()
    {
        // empty?
        if(!_tbl.wrap && _tbl.begin == _tbl.end)
            return true;
        
        record * rec = (record*)(_region_ptr + _tbl.begin);
        
        if(rec->expiry() == 1)
        {
            drop_head();
            return true;
        }
        
        // live record
        size_t key_length = rec->key_length();
        size_t val_length = rec->value_length();
        
        if(!would_fit(key_length, val_length))
            return false;
        
        size_t rec_len = key_length + val_length + record::header_size();
        
        // need to wrap?
        if(_tbl.end + rec_len > _tbl.region_size)
        {
            _tbl.wrap = _tbl.end;
            _tbl.end = records_offset();
        }
        
        // fill new record
        record * new_rec = (record*)(_region_ptr + _tbl.end);
        new (new_rec) record( rec->key(), rec->key() + key_length,
            rec->value(), rec->value() + val_length, rec->expiry());
        
        // Note: potential optimization would use get()
        //  to obtain &next => rec. Then, tracking prev
        //  would no longer be required.
        
        // rec->next => new_rec->next
        new_rec->set_next( rec->next());
        // rec->prev => new_rec->prev
        new_rec->set_prev( rec->prev());
        
        // new_rec => new_rec->next->prev
        if( new_rec->next())
            ((record*)(_region_ptr + new_rec->next()))->set_prev(
                (size_t) &new_rec->_next - (size_t) _region_ptr);
        // new_rec => new_rec->prev->next
        __RH_SET(_tbl.end, _region_ptr + new_rec->prev(), OffsetBytes);
        
        // update ring to reflect allocation
        _tbl.end += rec_len;
        
        // drop old rec
        drop_head();
        return true;
    }
    
    // least recently added/moved record
    //  is dropped. buffer-ring head steps.
    void drop_head()
    {
        // empty?
        if(!_tbl.wrap && _tbl.begin == _tbl.end)
            return;
        
        record * rec = (record*)(_region_ptr + _tbl.begin);
        size_t rlen = rec->key_length() + rec->value_length() + rec->header_size();
        
        _tbl.begin += rlen;
        if(_tbl.begin == _tbl.wrap)
        {
            _tbl.wrap = 0;
            _tbl.begin = records_offset();
        }
        
        if(rec->expiry() != 1)
            _tbl.record_count -= 1;
        return;
    }
    
private:
    
    size_t index_offset() const
    { return sizeof(table_header); }
    
    size_t records_offset() const
    { return sizeof(table_header) + _tbl.table_size * OffsetBytes; }
    
    struct table_header {
        
        unsigned int state;
        size_t region_size;
        size_t table_size;
        size_t record_count;
        
        // offset of first record
        size_t begin;
        // 1 beyond last record
        size_t end;
        // if end < begin, 1 beyond final record
        size_t wrap;
    };
    
    unsigned char * _region_ptr;
    table_header & _tbl;
};

}; // end namespace common

#endif



        /*
        #include <iostream>
        cout << "CURRENT TABLE STATE:" << endl;
        for(size_t bin = 0; bin != _tbl.table_size; ++bin)
        {
            size_t cur_off_off = index_offset() + bin * OffsetBytes;
            size_t cur_off;
            __RH_EXTRACT(cur_off, cur_off_off + _region_ptr, OffsetBytes);
            
            cout << "bin " << bin << ":" << endl;
            
            while(cur_off)
            {
                record * r = (record*)(_region_ptr + cur_off);
                size_t next_off = cur_off + r->key_length() + r->value_length() + r->header_size();
                cout << "\trec<" << cur_off << ":" << next_off << ">(";
                cout << r->prev() << ", " << r->next() << ", ";
                cout << r->key_length() << ", " << r->value_length();
                cout <<", " << r->expiry() << ") " << r->key() << " " << r->value() << endl;
                
                assert(cur_off_off == r->prev());
                cur_off_off = (size_t)(&r->_next) - (size_t)_region_ptr;
                cur_off = r->next();
            }
        }

        cout << endl << "CURRENT RECORD STATE:" << endl;
        size_t cur_off = _tbl.begin;
        while(cur_off != _tbl.end)
        {
            record * r = (record*)(_region_ptr + cur_off);
            size_t next_off = cur_off + r->key_length() + r->value_length() + r->header_size();
            cout << "\trec<" << cur_off << ":" << next_off << ">(";

            cout << r->prev() << ", " << r->next() << ", ";
            
            cout << r->key_length() << ", " << r->value_length();
            cout <<", " << r->expiry() << ") " << r->key() << " " << r->value() << endl;
            cur_off = next_off;
        }
        */
