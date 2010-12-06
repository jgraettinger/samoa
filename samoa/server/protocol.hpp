#ifndef SERVER_PROTOCOL_HPP
#define SERVER_PROTOCOL_HPP

#include "samoa/server/fwd.hpp"
#include <boost/unordered_map.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/noncopyable.hpp>

namespace server {

class protocol : private boost::noncopyable
{
public:

    typedef boost::shared_ptr<protocol> ptr_t;

    protocol() {}

    virtual ~protocol() { }

    virtual void start(const client_ptr_t & client) = 0;

    virtual void next_request(const client_ptr_t & client) = 0;

    void add_command_handler(const std::string & cmd_name,
        const command_handler_ptr_t & cmd);

    command_handler_ptr_t get_command_handler(const std::string &) const;

private:

    boost::unordered_map<std::string,
        command_handler_ptr_t> _cmd_table;
};

};

#endif

