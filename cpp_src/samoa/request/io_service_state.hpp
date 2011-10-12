#ifndef SAMOA_REQUEST_IO_SERVICE_STATE_HPP
#define SAMOA_REQUEST_IO_SERVICE_STATE_HPP

#include "samoa/core/proactor.hpp"

namespace samoa {
namespace request {

class io_service_state
{
public:

    io_service_state();

    virtual ~io_service_state();

    /*!
     * Retrieves the io-service.
     * Null until load_io_service_state()
     */
    const core::io_service_ptr_t & get_io_service() const
    { return _io_service; }

    void load_io_service_state(const core::io_service_ptr_t &);

    void reset_io_service_state();

private:

    core::io_service_ptr_t _io_service;
};

}
}

#endif

