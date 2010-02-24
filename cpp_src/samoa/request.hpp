#ifndef SAMOA_REQUEST_HPP
#define SAMOA_REQUEST_HPP

#include "samoa/fwd.hpp"
#include "common/ref_buffer.hpp"
#include "common/buffer_region.hpp"
#include <boost/shared_ptr.hpp>
#include <vector>

namespace samoa {

const size_t MAX_COMMAND_SIZE = 50;
const size_t MAX_DATA_SIZE    = (1<<30);

enum request_type {
    
    REQ_INVALID,
    
    REQ_PING,
    REQ_GET,
    REQ_FGET,
    REQ_SET
};

enum response_type {
    
    RESP_INVALID,
    
    RESP_OK_VERSION,
    RESP_OK_PONG,
    RESP_OK_SET,
    RESP_OK_GET_HIT,
    RESP_OK_GET_MISS,
    
    RESP_ERR_NO_SPACE,
    RESP_ERR_CMD_MALFORMED,
    
    // the connection will
    //  close after returning these
    RESP_ERR_CMD_OVERFLOW,
    RESP_ERR_DATA_MALFORMED,
    RESP_ERR_DATA_OVERFLOW
};

struct request {
    
    typedef boost::shared_ptr<request> ptr_t;
    
    request( const client_ptr_t & client);
    
    // re-inits request to just-constructed state
    void reset();
    
    request_type  req_type;
    response_type resp_type;
    
    // potential arguments of the request
    std::vector<char> key;
    size_t req_data_length;
    common::buffer_regions_t  req_data;
    
    // potential arguments of the response
    size_t resp_data_length;
    common::const_buffer_regions_t  resp_data;
    
    // client which issued/owns the request
    const client_ptr_t client;
};

};

#endif
