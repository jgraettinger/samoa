#ifndef SAMOA_CORE_PROTOBUF_SEQUENCE_UTIL_HPP
#define SAMOA_CORE_PROTOBUF_SEQUENCE_UTIL_HPP

#include "samoa/error.hpp"

namespace samoa {
namespace core {
namespace protobuf {

template<typename Sequence>
typename Sequence::value_type &
    insert_before(Sequence & seq, typename Sequence::iterator & iter)
{
    size_t ind = std::distance(seq.begin(), iter);

    // add at tail, and bubble into place
    seq.Add();
    for(size_t i = seq.size() - 1; i != ind; --i)
    {
        seq.SwapElements(i, i - 1);
    }

    // re-position iter to previous element
    iter = seq.begin() + ind + 1;
    return *seq.Mutable(ind);
}

template<typename Sequence>
typename Sequence::value_type &
    insert_before(Sequence & seq, unsigned index)
{
	typename Sequence::iterator iter = std::begin(seq) + index;
    return insert_before(seq, iter);
}

template<typename Sequence>
void remove_before(Sequence & seq, typename Sequence::iterator & iter)
{
    size_t ind = std::distance(seq.begin(), iter);
    SAMOA_ASSERT(ind);

    // bubble to tail, and remove
    for(size_t i = ind; i != (size_t) seq.size(); ++i)
    {
        seq.SwapElements(i, i - 1);
    }
    seq.RemoveLast();

    // re-position iter to previous element
    iter = seq.begin() + ind - 1;
}

template<typename Sequence>
void remove_before(Sequence & seq, unsigned index)
{
	typename Sequence::iterator iter = std::begin(seq) + index;
    remove_before(seq, iter);
}

}
}
}

#endif
