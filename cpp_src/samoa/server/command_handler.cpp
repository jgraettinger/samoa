#include "samoa/server/command_handler.hpp"
#include "samoa/request/request_state.hpp"
#include "samoa/request/state_exception.hpp"

namespace samoa {
namespace server {

void command_handler::checked_handle(const request::state::ptr_t & rstate)
{
    try
    {
        rstate->parse_samoa_request();
        handle(rstate);
    }
    catch(const request::state_exception & e)
    {
        rstate->send_error(e.get_code(), e.what());
    }
}

}
}

