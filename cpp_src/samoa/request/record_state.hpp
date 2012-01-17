#ifndef SAMOA_REQUEST_RECORD_STATE_HPP
#define SAMOA_REQUEST_RECORD_STATE_HPP

#include "samoa/core/protobuf/samoa.pb.h"
#include <boost/shared_ptr.hpp>

namespace samoa {
namespace request {

namespace spb = samoa::core::protobuf;

class record_state
{
public:

    typedef boost::shared_ptr<record_state> ptr_t;

    record_state();
    virtual ~record_state();

    spb::PersistedRecord & get_local_record()
    { return _local_record; }

    spb::PersistedRecord & get_remote_record()
    { return _remote_record; }

    void reset_record_state();

private:

    spb::PersistedRecord _local_record;
    spb::PersistedRecord _remote_record;
};

}
}

#endif

