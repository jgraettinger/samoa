#ifndef SAMOA_CLIENT_SERVER_POOL_HPP
#define SAMOA_CLIENT_SERVER_POOL_HPP

#include "samoa/client/fwd.hpp"
#include "samoa/client/server.hpp"
#include <boost/thread/mutex.hpp>
#include <boost/unordered_map.hpp>
#include <boost/shared_ptr.hpp>
#include <list>

namespace samoa {
namespace client {

class server_pool :
    public boost::enable_shared_from_this<server_pool>
{
public:

    typedef boost::shared_ptr<server_pool> ptr_t;

    server_pool(const core::proactor::ptr_t &);

    void schedule_request(const std::string & host, unsigned port,
        const server::request_callback_t &);

private:

    typedef std::pair<std::string, unsigned> key_t;

    void on_connect(const boost::system::error_code &,
        server::ptr_t, const key_t &);

    boost::mutex _mutex;
    core::proactor::ptr_t _proactor;

    typedef boost::unordered_map<key_t, server::ptr_t> server_map_t;
    server_map_t _servers;

    typedef std::list<server::request_callback_t> callback_list_t;
    typedef boost::unordered_map<key_t, callback_list_t> connecting_map_t;
    connecting_map_t _connecting;
};

}
}

#endif

