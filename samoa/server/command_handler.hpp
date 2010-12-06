#ifndef SERVER_COMMAND_HANDLER_HPP
#define SERVER_COMMAND_HANDLER_HPP

#include "samoa/server/fwd.hpp"

namespace server {

class command_handler : private boost::noncopyable
{
public:

    typedef boost::shared_ptr<command_handler> ptr_t;

    command_handler() {}

    virtual ~command_handler() {}

    virtual void handle(const client_ptr_t &, const context_ptr_t &) = 0;
};

}

#endif

