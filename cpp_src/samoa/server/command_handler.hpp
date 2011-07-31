#ifndef SAMOA_SERVER_COMMAND_HANDLER_HPP
#define SAMOA_SERVER_COMMAND_HANDLER_HPP

#include "samoa/server/fwd.hpp"
#include <boost/noncopyable.hpp>

namespace samoa {
namespace server {

class command_handler : private boost::noncopyable
{
public:

    typedef command_handler_ptr_t ptr_t;

    command_handler()
    { }

    virtual ~command_handler()
    { }

    virtual void handle(const client_ptr_t &) = 0;
};

}
}

#endif

