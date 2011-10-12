#ifndef SAMOA_REQUEST_STATE_EXCEPTION
#define SAMOA_REQUEST_STATE_EXCEPTION

#include <stdexcept>

namespace samoa {
namespace request {

class state_exception :
    public std::runtime_error
{
public:

    state_exception(unsigned code, const std::string & err)
     : std::runtime_error(err),
       _code(code)
    { }

    unsigned get_code() const
    { return _code; }

private:

    unsigned _code;
};

}
}

#endif

