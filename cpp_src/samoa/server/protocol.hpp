#ifndef SAMOA_SERVER_PROTOCOL_HPP
#define SAMOA_SERVER_PROTOCOL_HPP

#include "samoa/server/fwd.hpp"
#include <boost/noncopyable.hpp>
#include <vector>

namespace samoa {
namespace server {

class protocol : private boost::noncopyable
{
public:

    typedef protocol_ptr_t ptr_t;

    void set_command_handler(unsigned operation_type,
        const command_handler_ptr_t & handler)
    {
        if(_handler_table.size() <= operation_type)
            _handler_table.resize(operation_type * 2);
    
        _handler_table[operation_type] = handler;
    }

    const command_handler_ptr_t & get_command_handler(
        unsigned operation_type)
    { return _handler_table.at(operation_type); }

private:

    std::vector<command_handler_ptr_t> _handler_table;
};

}
}

#endif

