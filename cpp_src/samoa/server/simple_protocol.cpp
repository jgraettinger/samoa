
#include "samoa/server/simple_protocol.hpp"
#include "samoa/server/protocol.hpp"
#include "samoa/server/command_handler.hpp"
#include "samoa/server/client.hpp"
#include "samoa/core/ref_buffer.hpp"
#include <iostream>

namespace samoa {
namespace server {

// Canned responses
namespace {
    const size_t MAX_COMMAND_LENGTH = 128;
    core::const_buffer_region _resp_newline("\r\n");
    core::const_buffer_region _resp_version("SAMOA v0.1\r\n");
    core::const_buffer_region _resp_err_cmd_overflow("-ERR command overflow\r\n");
    core::const_buffer_region _resp_err_cmd_unknown("-ERR command unknown\r\n");
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
        const core::buffers_iterator_t & begin,
        core::buffers_iterator_t end)
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

    // strip trailing newline & possibly carriage return
    --end; if(end != begin && *(end-1) == '\r') --end;

    command_handler::ptr_t handler = get_command_handler(begin, end);

    if(!handler.get())
    {
        // invalid command
        client->queue_write(_resp_err_cmd_unknown);
        client->write_queued(boost::bind(&simple_protocol::on_write,
            shared_from_this(), client, _1, false));
        return;
    }

    handler->handle(client);
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

    std::cout << "simple_protocol::on_write()" << std::endl;
}

}
}

