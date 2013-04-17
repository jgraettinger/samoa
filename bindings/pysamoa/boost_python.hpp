#ifndef PYSAMOA_BOOST_PYTHON_HPP
#define PYSAMOA_BOOST_PYTHON_HPP

#include <memory>

namespace boost {
    template<class T> const T* get_pointer(const std::shared_ptr<const T>& ptr)
    {
        return ptr.get();
    }
    template<class T> T* get_pointer(const std::shared_ptr<T>& ptr)
    {
        return ptr.get();
    }
}

#include <boost/python.hpp>

#endif
