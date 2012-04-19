#ifndef SAMOA_CORE_BUFFER_POOL_HPP
#define SAMOA_CORE_BUFFER_POOL_HPP

#include <boost/detail/atomic_count.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/noncopyable.hpp>
#include <new>

namespace samoa {
namespace core {

class ref_buffer;
void intrusive_ptr_add_ref(const ref_buffer *);
void intrusive_ptr_release(const ref_buffer *);

class ref_buffer : private boost::noncopyable
{
public:

    const static unsigned allocation_size;

    typedef boost::intrusive_ptr<ref_buffer> ptr_t;
    typedef boost::intrusive_ptr<const ref_buffer> const_ptr_t;
    
    char * data()
    { return _data; }
    
    const char * data() const
    { return _data; }
    
    size_t size() const
    { return _size; }
    
    static ptr_t aquire_ref_buffer(size_t size)
    {
        void * buf = new char[sizeof(ref_buffer) + size];
        return ptr_t(new (buf) ref_buffer(size));
    }
    
private:
    
    ref_buffer(size_t size)
     : _size(size), _ref_cnt(0)
    { }
    
    ~ref_buffer()
    { }
    
    const size_t  _size;
    mutable boost::detail::atomic_count _ref_cnt;
    char          _data[];
    
    friend void intrusive_ptr_add_ref(const ref_buffer *);
    friend void intrusive_ptr_release(const ref_buffer *);
};

inline void intrusive_ptr_add_ref(const ref_buffer * cp)
{ ++(cp->_ref_cnt); }

inline void intrusive_ptr_release(const ref_buffer * cp)
{
    if( --(cp->_ref_cnt) == 0)
    {
        cp->~ref_buffer();
        delete [] (char*) cp;
    }
}

}
}

#endif

