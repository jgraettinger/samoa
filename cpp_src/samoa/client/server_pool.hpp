#ifndef SAMOA_CLIENT_SERVER_POOL_HPP
#define SAMOA_CLIENT_SERVER_POOL_HPP

#include "samoa/client/server.hpp"
#include "samoa/client/fwd.hpp"
#include "samoa/core/fwd.hpp"
#include "samoa/core/uuid.hpp"
#include "samoa/spinlock.hpp"
#include <boost/unordered_map.hpp>
#include <memory>
#include <list>

namespace samoa {
namespace client {

class server_pool :
    public boost::enable_shared_from_this<server_pool>
{
public:

    typedef server_pool_ptr_t ptr_t;

    server_pool();
    virtual ~server_pool();

    // Submits the request via server::schedule_request to a
    //  connected server instance. If none is available, a
    //  connection is first established.
    void schedule_request(const server::request_callback_t &,
        const core::uuid & server_uid);

    // Returns a server only if a connected instance is available
    server::ptr_t get_server(const core::uuid & server_uid);

    // Closes all currently-connected server instances
    //   Connections being established are unaffected, and
    //   further use of schedule_request() will result in
    //   connections being re-established
    void close();

    /// A server address must be declared before requests can
    ///  be submitted under the server uuid.
    void set_server_address(const core::uuid & server_uid,
        const std::string & host, unsigned short port);

    /// Adds an existing, connected server instance to the pool.
    void set_connected_server(const core::uuid & server_uid,
        const server::ptr_t & server);

private:

    void on_connect(const boost::system::error_code &,
        server::ptr_t, const core::uuid &);

    spinlock _lock;
    
    typedef std::pair<std::string, unsigned short> address_t;
    typedef boost::unordered_map<core::uuid, address_t> address_map_t;
    address_map_t _addresses;

    typedef boost::unordered_map<core::uuid, server::ptr_t> server_map_t;
    server_map_t _servers;

    typedef std::list<server::request_callback_t> callback_list_t;
    typedef boost::unordered_map<core::uuid, callback_list_t> connecting_map_t;
    connecting_map_t _connecting;

    // proactor lifetime management
    core::proactor_ptr_t _proactor;
};

}
}

#endif

