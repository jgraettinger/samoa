#include "samoa/request/io_service_state.hpp"

namespace samoa {
namespace request {

io_service_state::io_service_state()
{ }

io_service_state::~io_service_state()
{ }

void io_service_state::load_io_service_state(
    const core::io_service_ptr_t & io_service)
{
    _io_service = io_service;
}

void io_service_state::reset_io_service_state()
{
    _io_service.reset();
}

}
}

