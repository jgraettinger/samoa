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
    REQ_SET,
    REQ_ITER_KEYS
};

struct request {
    
    typedef boost::shared_ptr<request> ptr_t;
    
    request()
     :  req_type(REQ_INVALID),
        req_data_length(0),
        close_on_finish(false),
        cur_rec(0)
    { }
    
    // re-inits request to just-constructed state
    void reset()
    {
        req_type = REQ_INVALID;
        req_data_length  = 0;
        key.clear();
        req_data.clear();
        close_on_finish = false;
        cur_rec = 0;
        return;
    }
    
    request_type  req_type;
    
    // potential arguments of the request
    std::vector<char> key;
    size_t req_data_length;
    common::buffer_regions_t  req_data;
    
    // potential arguments of the response
    bool close_on_finish;
    const rolling_hash_t::record * cur_rec;
};

};

#endif
