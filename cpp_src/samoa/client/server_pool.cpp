
#include "samoa/client/server_pool.hpp"
#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>

namespace samoa {
namespace client {

server_pool::server_pool(const core::proactor::ptr_t & p)
 : _proactor(p)
{ }

void server_pool::schedule_request(const std::string & host, unsigned port,
    const server::request_callback_t & callback)
{
    key_t key(host, port);

    boost::mutex::scoped_lock guard(_mutex);

    // already have a connected server?
    {
        server_map_t::const_iterator it = _servers.find(key);

        if(it != _servers.end() && it->second->is_open())
        {
            it->second->schedule_request(callback);
            return;
        }
    }

    // lookup list of request callbacks waiting on this server
    std::list<server::request_callback_t> & pending_callbacks(
        _connecting[key]);

    // empty list? this is the first attempt to use this server
    if(pending_callbacks.empty())
    {
        //  start a new connection
        server::connect_to(_proactor, host,
            boost::lexical_cast<std::string>(port),
            boost::bind(&server_pool::on_connect, shared_from_this(),
                _1, _2, key));
    }

    pending_callbacks.push_back(callback);
}

void server_pool::on_connect(const boost::system::error_code & ec,
    server::ptr_t server, const server_pool::key_t & key)
{
    callback_list_t callbacks;
    {
        boost::mutex::scoped_lock guard(_mutex);

        // move list of callbacks into local variable,
        //   clearing the original
        {
            connecting_map_t::iterator it = _connecting.find(key);
            assert(it != _connecting.end());

            callbacks.swap(it->second);
            _connecting.erase(it);
        }

        if(!ec)
        {
            // sucessfully connected
            _servers[key] = server;

            // pass request callbacks to server instance
            for(callback_list_t::iterator it = callbacks.begin();
                it != callbacks.end(); ++it)
            {
                server->schedule_request(*it);
            }
        }
    }

    if(ec)
    {
        // errorback *outside* of lock context,
        //   (the callback may make new requests) 

        for(callback_list_t::iterator it = callbacks.begin();
            it != callbacks.end(); ++it)
        {
            (*it)(ec, server_request_interface::null_instance());
        }
    }
}


}
}
