
#include "samoa/client/server_pool.hpp"
#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>

namespace samoa {
namespace client {

server_pool::server_pool(const core::proactor::ptr_t & p)
 : _proactor(p)
{ }

server_pool::~server_pool()
{ }

void server_pool::set_server_address(const core::uuid & uuid,
    const std::string & host, unsigned port)
{
    _addresses[uuid] = address_t(host, port);
    _servers[uuid] = server::ptr_t();
}

void server_pool::set_connected_server(const core::uuid & uuid,
    const server::ptr_t & server)
{
    _servers[uuid] = server;
}

void server_pool::schedule_request(const core::uuid & uuid,
    const server::request_callback_t & callback)
{
    // already have a connected server?
    {
        server_map_t::const_iterator it = _servers.find(uuid);

        if(it != _servers.end() && it->second && it->second->is_open())
        {
            it->second->schedule_request(callback);
            return;
        }
    }

    boost::mutex::scoped_lock guard(_mutex);

    // assert that an address exists for this server
    address_map_t::const_iterator addr_it = _addresses.find(uuid);
    if(addr_it == _addresses.end())
        throw std::runtime_error("server address not set");

    // lookup list of request callbacks waiting on this server
    std::list<server::request_callback_t> & pending_callbacks(
        _connecting[uuid]);

    // empty list? this is the first attempt to use this server
    if(pending_callbacks.empty())
    {
        //  start a new connection
        server::connect_to(_proactor, addr_it->second.first,
            boost::lexical_cast<std::string>(addr_it->second.second),
            boost::bind(&server_pool::on_connect, shared_from_this(),
                _1, _2, uuid));
    }
    pending_callbacks.push_back(callback);
}

server::ptr_t server_pool::get_server(const core::uuid & uuid)
{
    server_map_t::const_iterator it = _servers.find(uuid);

    if(it != _servers.end() && it->second && it->second->is_open())
        return it->second;
    else
        return server::ptr_t();
}

void server_pool::close()
{
    for(server_map_t::const_iterator it = _servers.begin();
        it != _servers.end(); ++it)
    {
        it->second->close();
    }
}

void server_pool::on_connect(const boost::system::error_code & ec,
    server::ptr_t server, const core::uuid & uuid)
{
    callback_list_t callbacks;
    {
        boost::mutex::scoped_lock guard(_mutex);

        // move list of callbacks into local variable,
        //   clearing the original
        {
            connecting_map_t::iterator it = _connecting.find(uuid);
            assert(it != _connecting.end());

            callbacks.swap(it->second);
            _connecting.erase(it);
        }

        if(!ec)
        {
            // sucessfully connected
            _servers[uuid] = server;

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
