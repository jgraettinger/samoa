
#include "samoa/server/simple_protocol.hpp"
#include "samoa/server/protocol.hpp"
#include "samoa/server/client.hpp"
#include "samoa/common/ref_buffer.hpp"
#include <boost/python.hpp>
#include <iostream>

namespace server {

using namespace boost::python;

// Canned responses
namespace {
    const size_t MAX_COMMAND_LENGTH = 128;
    common::const_buffer_region _resp_newline("\r\n");
    common::const_buffer_region _resp_version("SAMOA v0.1\r\n");
    common::const_buffer_region _resp_err_cmd_overflow("-ERR command overflow\r\n");
    common::const_buffer_region _resp_err_cmd_unknown("-ERR command unknown\r\n");
};

void simple_protocol::start(const client::ptr_t & client)
{
    client->queue_write(_resp_version);
    client->write_queued(boost::bind(&simple_protocol::on_write,
        shared_from_this(), client, _1, false));
}

void simple_protocol::next_request(const client::ptr_t & client)
{
    client->read_until('\n', MAX_COMMAND_LENGTH, boost::bind(
        &simple_protocol::on_command, shared_from_this(),
        client, _1, _2, _3));
}

void simple_protocol::on_command(const client::ptr_t & client,
        const boost::system::error_code & ec,
        const common::buffers_iterator_t & begin,
        const common::buffers_iterator_t & end)
{
    if(ec == boost::system::errc::value_too_large)
    {
        client->queue_write(_resp_err_cmd_overflow);
        client->write_queued(boost::bind(&simple_protocol::on_write,
            shared_from_this(), client, _1, true));
        return;
    }
    else if(ec)
    {
        std::cerr << "simple_protocol::on_command: " << ec.message() << std::endl;
        client->socket().close();
        return;
    }

    std::string cmd(begin, end);

    client->queue_write("Got command ");
    client->queue_write(begin, end);
    client->queue_write(_resp_newline);
    client->write_queued(boost::bind(&simple_protocol::on_write,
        shared_from_this(), client, _1, false));

    return;
}

void simple_protocol::on_write(const client::ptr_t & client,
    const boost::system::error_code & ec, bool close)
{
    if(ec)
    {
        std::cerr << "simple_protocol::on_write: " << ec.message() << std::endl;
        client->socket().close();
        return;
    }
    if(!close)
        next_request(client);
}

void make_simple_protocol_bindings()
{
    class_<simple_protocol, simple_protocol::ptr_t, boost::noncopyable,
        bases<protocol> >("simple_protocol", init<>());
}

}

