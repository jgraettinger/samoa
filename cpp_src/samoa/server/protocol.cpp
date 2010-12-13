
#include "samoa/server/protocol.hpp"
#include "samoa/server/command_handler.hpp"

namespace samoa {
namespace server {

typedef std::pair<std::string, command_handler::ptr_t> cmd_row;
typedef std::vector<cmd_row> cmd_table_t;
typedef std::pair<const core::buffers_iterator_t &,
    const core::buffers_iterator_t &> it_pair;

struct cmd_order_pred
{
    bool operator()(const cmd_row & l, const std::string & r)
    { return l.first < r; }

    bool operator()(const cmd_row & l, const it_pair & r)
    {
        return std::lexicographical_compare(
            l.first.begin(), l.first.end(), r.first, r.second);
    }

    bool operator()(const std::string & l, const cmd_row & r)
    { return l < r.first; }

    bool operator()(const it_pair & l, const cmd_row & r)
    {
        return std::lexicographical_compare(
            l.first, l.second, r.first.begin(), r.first.end());
    }
};

void protocol::add_command_handler(const std::string & cmd_name,
    const command_handler::ptr_t & handler)
{
    cmd_table_t::iterator it = std::lower_bound(
        _cmd_table.begin(), _cmd_table.end(), cmd_name, cmd_order_pred());

    if(it != _cmd_table.end() && it->first == cmd_name)
    {
        throw std::runtime_error(
            "command " + cmd_name + " already has a handler");
    }

    _cmd_table.insert(it, std::make_pair(cmd_name, handler));
}

command_handler_ptr_t protocol::get_command_handler(
    const core::buffers_iterator_t & cmd_beg,
    const core::buffers_iterator_t & cmd_end) const
{
    cmd_table_t::const_iterator it = std::lower_bound(
        _cmd_table.begin(), _cmd_table.end(), it_pair(cmd_beg, cmd_end),
        cmd_order_pred());

    if(it != _cmd_table.end() &&
        (unsigned)std::distance(cmd_beg, cmd_end) == it->first.size() &&
        std::equal(it->first.begin(), it->first.end(), cmd_beg))
        return it->second;

    return command_handler_ptr_t();
}

command_handler_ptr_t protocol::get_command_handler(
    const std::string & cmd_name) const
{
    cmd_table_t::const_iterator it = std::lower_bound(
        _cmd_table.begin(), _cmd_table.end(), cmd_name, cmd_order_pred());

    if(it != _cmd_table.end() && it->first == cmd_name)
        return it->second;

    return command_handler_ptr_t();
}

}
}

