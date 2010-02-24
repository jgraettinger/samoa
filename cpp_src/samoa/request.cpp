
#include "samoa/request.hpp"

namespace samoa {

request::request(const client_ptr_t & client)
 : req_type(REQ_INVALID),
   resp_type(RESP_INVALID),
   req_data_length(0),
   resp_data_length(0),
   client(client)
{ }

void request::reset()
{
    req_type  = REQ_INVALID;
    resp_type = RESP_INVALID;
    req_data_length  = 0;
    resp_data_length = 0;
    key.clear();
    req_data.clear();
    resp_data.clear();
    return;
}

};

