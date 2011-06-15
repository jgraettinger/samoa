#ifndef SAMOA_ERROR_HPP
#define SAMOA_ERROR_HPP

#include <boost/system/system_error.hpp>
#include <boost/system/error_code.hpp>

#define SAMOA_ASSERT(arg)\
{\
    if(__builtin_expect(!(bool)(arg), 0))\
    {\
        throw samoa::error::samoa_exception(\
            "assertion_failure",\
            #arg,\
            __FILE__,\
            __PRETTY_FUNCTION__,\
            __LINE__,\
            false);\
    }\
}

#define SAMOA_ASSERT_ERRNO(arg)\
{\
    int __res = (arg);\
    if(__builtin_expect(__res, 0))\
    {\
        throw samoa::error::samoa_exception(\
            "assertion_failure",\
            std::string(#arg " => ") + boost::system::system_error(__res,\
                boost::system::system_category()).what(),\
            __FILE__,\
            __PRETTY_FUNCTION__,\
            __LINE__,\
            false);\
    }\
}

#define SAMOA_ABORT_IF(arg)\
{\
    if(__builtin_expect(!(bool)(arg), 0))\
    {\
        samoa::error::generate_core_file();\
        throw samoa::error::samoa_exception(\
            "assertion_failure",\
            #arg,\
            __FILE__,\
            __PRETTY_FUNCTION__,\
            __LINE__,\
            true);\
    }\
}

#define SAMOA_ABORT_ERRNO(arg)\
{\
    int __res = (arg);\
    if(__builtin_expect(__res, 0))\
    {\
        samoa::error::generate_core_file();\
        throw samoa::error::samoa_exception(\
            "assertion_failure",\
            std::string(#arg " => ") + boost::system::system_error(__res,\
                boost::system::system_category()).what(),\
            __FILE__,\
            __PRETTY_FUNCTION__,\
            __LINE__,\
            false);\
    }\
}

namespace samoa {
namespace error {

class samoa_exception : public std::exception
{
public:

    samoa_exception(
        const std::string & type,
        const std::string & msg,
        const std::string & file = "",
        const std::string & func = "",
        unsigned line_no = 0,
        bool abort = false);

    ~samoa_exception() throw()
    {}

    virtual const char * what() const throw()
    { return _what.c_str(); }

    const std::string type;
    const std::string msg;
    const std::string file;
    const std::string func;
    const unsigned line_no;
    const bool abort;

private:

    std::string _what;
};

void throw_not_found(const std::string & what,
    const std::string & identity);

void generate_core_file();

}
}

#endif
