
#ifndef SAMOA_PARTITION_HPP
#define SAMOA_PARTITION_HPP

#include "samoa/fwd.hpp"
#include <boost/system/error_code.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/shared_ptr.hpp>
#include <string>
#include <set>

namespace samoa {

class partition :
    public boost::enable_shared_from_this<partition>
{
public:
    
    partition(const std::string & file, size_t region_size, size_t table_size);
    
    void handle_request(const client_ptr_t &);
    
private:
    
    void on_iteration(const boost::system::error_code & ec, const client_ptr_t &);
    
    rolling_hash_ptr_t _table;
    
    std::set<client_ptr_t> _iterating_clients;
};

};

#endif

