#ifndef SAMOA_SERVER_COMMAND_HANDLER_HPP
#define SAMOA_SERVER_COMMAND_HANDLER_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/request/fwd.hpp"
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

    /*!
     * Dispatches command handler.
     *
     * In particular, checked_handle:
     *  - invokes request::state::validate_samoa_request_syntax()
     *  - invokes virtual handle()
     *
     * If request::state_exception is thrown by handle(),
     *  it's caught within checked_handle and resent to
     *  the client via request::state::send_error.
     *
     * Ie, handle() need not catch state_exceptions itself
     *  while validating the request.
     *
     * Note this is _ONLY TRUE_ of the initial invocation
     *  into handle(); any callbacks which may indirectly
     *  throw a state_exception will need to catch it
     *  themselves.
     */
    void checked_handle(const request::state_ptr_t &);

protected:

    virtual void handle(const request::state_ptr_t &) = 0;
};

}
}

#endif

