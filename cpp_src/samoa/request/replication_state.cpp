
#include "samoa/request/replication_state.hpp"
#include "samoa/request/state_exception.hpp"

namespace samoa {
namespace request {

replication_state::replication_state()
 :  _replication_factor(0),
    _quorum_count(0),
    _failure_count(0),
    _success_count(0)
{ }

replication_state::~replication_state()
{ }

void replication_state::set_quorum_count(unsigned quorum_count)
{
    _quorum_count = quorum_count;
}

bool replication_state::peer_replication_failure()
{
    // did we already succeed?
    if(_success_count == _quorum_count)
    {
        return false;
    }

    ++_failure_count;

    // is this the last outstanding replication?
    return _failure_count + _success_count == _replication_factor;
}

bool replication_state::peer_replication_success()
{
    // did we already succeed?
    if(_success_count == _quorum_count)
    {
        return false;
    }

    // did we just succeed?
    if(++_success_count == _quorum_count)
    {
        return true;
    }

    // is this the last outstanding replication?
    return _failure_count + _success_count == _replication_factor;
}

bool replication_state::is_replication_finished() const
{
    return _success_count >= _quorum_count ||
        _failure_count + _success_count == _replication_factor;
}

void replication_state::load_replication_state(unsigned replication_factor)
{
    if(_quorum_count > replication_factor)
    {
        throw state_exception(400, "quorum too large");
    }
    if(_quorum_count == 0)
    {
        _quorum_count = replication_factor;
    }

    _replication_factor = replication_factor;
}

void replication_state::reset_replication_state()
{
    _replication_factor = 0;
    _quorum_count = 0;
    _failure_count = 0;
    _success_count = 0;
}

}
}

