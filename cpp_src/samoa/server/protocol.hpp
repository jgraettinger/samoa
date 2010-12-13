#ifndef SAMOA_SERVER_PROTOCOL_HPP
#define SAMOA_SERVER_PROTOCOL_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/core/buffer_region.hpp"
#include <boost/enable_shared_from_this.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/noncopyable.hpp>
#include <vector>

namespace samoa {
namespace server {

class protocol : private boost::noncopyable
{
public:

    typedef boost::shared_ptr<protocol> ptr_t;

    protocol() {}

    virtual ~protocol() {}

    virtual void start(const client_ptr_t & client) = 0;

    virtual void next_request(const client_ptr_t & client) = 0;

    void add_command_handler(const std::string & cmd_name,
        const command_handler_ptr_t & handler);

    command_handler_ptr_t get_command_handler(
        const core::buffers_iterator_t & cmd_begin,
        const core::buffers_iterator_t & cmd_end) const;

    command_handler_ptr_t get_command_handler(const std::string &) const;

private:

    std::vector<std::pair<std::string, command_handler_ptr_t> > _cmd_table;
};

}
}

#endif

