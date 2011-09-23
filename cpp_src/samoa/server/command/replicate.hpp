#ifndef SAMOA_SERVER_COMMAND_BASIC_REPLICATE_HPP
#define SAMOA_SERVER_COMMAND_BASIC_REPLICATE_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/server/command_handler.hpp"
#include "samoa/datamodel/merge_func.hpp"
#include <boost/smart_ptr/enable_shared_from_this.hpp>
#include <boost/system/error_code.hpp>
#include <boost/shared_ptr.hpp>

namespace samoa {
namespace server {
namespace command {

namespace spb = samoa::core::protobuf;

class replicate_handler :
    public command_handler,
    public boost::enable_shared_from_this<replicate_handler>
{
public:

    typedef boost::shared_ptr<replicate_handler> ptr_t;

    replicate_handler()
    { }

    void handle(const request_state_ptr_t &);

protected:

    void on_write(const boost::system::error_code &,
        const datamodel::merge_result &, const request_state_ptr_t &);

    void on_read(const boost::system::error_code &,
        bool, const request_state_ptr_t &);

    void on_reverse_replication();
};

}
}
}

#endif

