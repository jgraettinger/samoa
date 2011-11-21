#ifndef SAMOA_PERSISTENCE_ROLLING_HASH_VALUE_GATHER_ITERATOR_HPP
#define SAMOA_PERSISTENCE_ROLLING_HASH_VALUE_GATHER_ITERATOR_HPP

#include "samoa/persistence/rolling_hash/fwd.hpp"
#include "samoa/persistence/rolling_hash/element.hpp"
#include <boost/iterator/iterator_facade.hpp>

namespace samoa {
namespace persistence {
namespace rolling_hash {

class value_gather_iterator :
    public boost::iterator_facade<
        value_gather_iterator, const char, boost::forward_traversal_tag>
{
public:

    value_gather_iterator()
     :  _element(nullptr),
        _next(nullptr),
        _cur(nullptr),
        _end(nullptr)
    { }

    value_gather_iterator(element & elem)
     : _element(&elem),
       _next(_element->head()),
       _cur(nullptr),
       _end(nullptr)
    {
        increment();    
    }

private:

    friend class boost::iterator_core_access;

    void increment()
    {
        if(++_cur >= _end)
        {
            while(_next)
            {
                _cur = _next->value_begin();
                _end = _next->value_end();

                _next = _element->step(_next);

                if(_cur != _end)
                    return;
            }
            _cur = _end = nullptr;
        }
    }

    bool equal(const value_gather_iterator & other) const
    { return _cur == other._cur; }

    const char & dereference() const
    { return *_cur; }

    element * _element;
    packet * _next;
    const char * _cur, * _end; 
};

}
}
}

#endif


