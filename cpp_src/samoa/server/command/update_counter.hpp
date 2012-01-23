#ifndef SAMOA_SERVER_COMMAND_UPDATE_COUNTER_HPP
#define SAMOA_SERVER_COMMAND_UPDATE_COUNTER_HPP

#include "samoa/persistence/fwd.hpp"
#include "samoa/server/fwd.hpp"
#include "samoa/server/command_handler.hpp"
#include "samoa/server/table.hpp"
#include "samoa/persistence/fwd.hpp"
#include "samoa/datamodel/merge_func.hpp"
#include "samoa/request/fwd.hpp"
#include "samoa/core/protobuf/fwd.hpp"
#include "samoa/core/uuid.hpp"
#include <boost/system/error_code.hpp>
#include <boost/smart_ptr/enable_shared_from_this.hpp>
#include <vector>

namespace samoa {
namespace server {
namespace command {

namespace spb = samoa::core::protobuf;

class update_counter_handler :
    public command_handler,
    public boost::enable_shared_from_this<update_counter_handler>
{
public:

    typedef boost::shared_ptr<update_counter_handler> ptr_t;

    update_counter_handler()
    { }

    void handle(const request::state_ptr_t &);

private:

    datamodel::merge_result on_merge(spb::PersistedRecord &,
        const spb::PersistedRecord &, const request::state_ptr_t &);

    void on_put(const boost::system::error_code &,
        const datamodel::merge_result &, const request::state_ptr_t &);

    void on_replicated_write(const request::state_ptr_t &);
};

}
}
}

#endif

