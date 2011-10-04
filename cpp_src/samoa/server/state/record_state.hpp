#ifndef SAMOA_SERVER_STATE_RECORD_STATE_HPP
#define SAMOA_SERVER_STATE_RECORD_STATE_HPP

#include "samoa/core/protobuf/samoa.pb.h"
#include <boost/shared_ptr.hpp>

namespace samoa {
namespace server {
namespace state {

namespace spb = samoa::core::protobuf;

class record_state
{
public:

    typedef boost::shared_ptr<record_state> ptr_t;

    spb::PersistedRecord & get_local_record()
    { return _local_record; }

    spb::PersistedRecord & get_remote_record()
    { return _remote_record; }

    void reset_record_state()
    {
        _local_record.Clear();
        _remote_record.Clear();
    }

private:

    spb::PersistedRecord _local_record;
    spb::PersistedRecord _remote_record;
};

}
}
}

#endif

