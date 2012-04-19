
#include "samoa/client/server_pool.hpp"
#include "samoa/error.hpp"
#include "samoa/log.hpp"
#include <boost/lexical_cast.hpp>
#include <functional>

namespace samoa {
namespace client {

server_pool::server_pool()
 : _proactor(core::proactor::get_proactor())
{ }

server_pool::~server_pool()
{ }

void server_pool::set_server_address(const core::uuid & uuid,
    const std::string & host, unsigned short port)
{
    spinlock::guard guard(_lock);

    _addresses[uuid] = address_t(host, port);
    _servers[uuid] = server::ptr_t();
}

void server_pool::set_connected_server(const core::uuid & uuid,
    const server::ptr_t & server)
{
    spinlock::guard guard(_lock);

    SAMOA_ASSERT(_addresses.find(uuid) != _addresses.end());

    _servers[uuid] = server;
}

bool server_pool::has_server(const core::uuid & uuid)
{
    spinlock::guard guard(_lock);

    return _addresses.find(uuid) != _addresses.end();
}

server::ptr_t server_pool::get_server(const core::uuid & uuid)
{
    spinlock::guard guard(_lock);

    server_map_t::const_iterator it = _servers.find(uuid);
    SAMOA_ASSERT(it != _servers.end());

    if(it->second && it->second->is_open())
        return it->second;
    else
        return server::ptr_t();
}

std::string server_pool::get_server_hostname(const core::uuid & uuid)
{
    spinlock::guard guard(_lock);

    address_map_t::const_iterator it = _addresses.find(uuid);
    SAMOA_ASSERT(it != _addresses.end());

    return it->second.first;
}

unsigned short server_pool::get_server_port(const core::uuid & uuid)
{
    spinlock::guard guard(_lock);

    address_map_t::const_iterator it = _addresses.find(uuid);
    SAMOA_ASSERT(it != _addresses.end());

    return it->second.second;
}

void server_pool::connect()
{
    spinlock::guard guard(_lock);

    for(address_map_t::value_type & entry : _addresses)
    {
        const core::uuid & uuid = entry.first;
        server_map_t::const_iterator it = _servers.find(uuid);

        if(it != _servers.end() && it->second && it->second->is_open())
        {
            // already connected
            continue;
        }

        if(_connecting.find(uuid) != _connecting.end())
        {
            // connection already in-progress
            continue;   
        }

        server::connect_to(
            std::bind(&server_pool::on_connect, shared_from_this(),
                _1, _2, uuid),
            entry.second.first, entry.second.second);

        // implicit insert
        _connecting[uuid];
    }
}

void server_pool::schedule_request(
    const server::request_callback_t & callback,
    const core::uuid & uuid)
{
    spinlock::guard guard(_lock);

    // already have a connected server?
    {
        server_map_t::const_iterator it = _servers.find(uuid);

        if(it != _servers.end() && it->second && it->second->is_open())
        {
            it->second->schedule_request(callback);
            return;
        }
    }

    address_map_t::const_iterator addr_it = _addresses.find(uuid);

    SAMOA_ASSERT(addr_it != _addresses.end());

    // lookup queue of requests pending on this server's connection
    connecting_map_t::iterator conn_it = _connecting.find(uuid);

    // no connection attempt is in progress; start one
    if(conn_it == _connecting.end())
    {
        server::connect_to(
            std::bind(&server_pool::on_connect, shared_from_this(),
                _1, _2, uuid),
            addr_it->second.first,
            addr_it->second.second);

        _connecting[uuid].push_back(callback);
    }
    else
        conn_it->second.push_back(callback);
}

void server_pool::close()
{
    spinlock::guard guard(_lock);

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
    address_t addr;
    {
        spinlock::guard guard(_lock);

        // move list of callbacks into local variable,
        //   clearing the original
        {
            connecting_map_t::iterator it = _connecting.find(uuid);
            assert(it != _connecting.end());

            callbacks.swap(it->second);
            _connecting.erase(it);
        }

        addr = _addresses[uuid];

        if(!ec)
        {
            LOG_INFO("connected to peer " << uuid << "@"
                << addr.first << ":" << addr.second);

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
        LOG_INFO("connection to peer " << uuid << "@"
            << addr.first << ":" << addr.second << "failed: " << ec.message());

        // errorback *outside* of lock context,
        //   (the callback may make new requests) 

        for(callback_list_t::iterator it = callbacks.begin();
            it != callbacks.end(); ++it)
        {
            server_request_interface null;
            (*it)(ec, null);
        }
    }
}


}
}
