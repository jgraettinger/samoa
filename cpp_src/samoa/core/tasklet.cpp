
#include "samoa/core/tasklet_group.hpp"
#include "samoa/core/tasklet.hpp"
#include "samoa/core/proactor.hpp"
#include "samoa/error.hpp"
#include "samoa/log.hpp"

namespace samoa {
namespace core {

tasklet_base::tasklet_base(const core::io_service_ptr_t & io_service)
 : _io_service(io_service)
{ }

tasklet_base::~tasklet_base()
{
    if(!_group)
    {
        LOG_WARN("destroying unstarted tasklet " + _name);
    }
    else
    {
        _group->tasklet_destroyed(_name);
        LOG_DBG("destroying tasklet " + _name);
    }
}

void tasklet_base::set_tasklet_name(std::string && name)
{
    SAMOA_ASSERT(_name.empty());
    SAMOA_ASSERT(!name.empty());

    _name = std::move(name);
}

}
}

