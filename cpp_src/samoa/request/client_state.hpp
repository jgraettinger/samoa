#ifndef SAMOA_REQUEST_CLIENT_STATE_HPP
#define SAMOA_REQUEST_CLIENT_STATE_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/server/client.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include "samoa/core/buffer_region.hpp"
#include <boost/shared_ptr.hpp>

namespace samoa {
namespace request {

namespace spb = samoa::core::protobuf;

class client_state
{
public:

    client_state();

    virtual ~client_state();

    /*!
     * Retrives the client of the request
     */
    const server::client_ptr_t & get_client() const
    { return _client; }

    /*!
     * Retrieves the const protobuf SamoaRequest
     */
    const spb::SamoaRequest & get_samoa_request() const
    { return _samoa_request; }

    /*!
     * Retrieves the non-const protobuf SamoaRequest;
     *  For use in filling out the description
     */
    spb::SamoaRequest & mutable_samoa_request()
    { return _samoa_request; }

    /*!
     * Validates the syntax of the SamoaRequest; specifically:
     *
     *  - asserts that all UUID fields are parseable
     *  - asserts the ClusterClock (if set) is valid
     */
    void validate_samoa_request_syntax();

    /*!
     * Retrives the (const) data-blocks which accompany the client's request
     */
    const std::vector<core::buffer_regions_t> & get_request_data_blocks() const
    { return _request_data_blocks; }

    /*!
     * Retrives the (mutable) data-blocks; for use in fillout out the container
     */
    std::vector<core::buffer_regions_t> & mutable_request_data_blocks()
    { return _request_data_blocks; }

    /*!
     * Retrieves the (mutable) protobuf SamoaResponse
     *
     * Note: As SamoaResponse is modified everywhere, a const-protected
     *   accessor is not provided.
     */
    spb::SamoaResponse & get_samoa_response()
    { return _samoa_response; }

    /*!
     * \brief Adds the const buffer-regions as a response datablock
     *
     * SamoaResponse::data_block_length is appropriately updated.
     */
    void add_response_data_block(const core::const_buffer_regions_t &);

    /*!
     * \brief Adds the buffer-regions as a response datablock
     *
     * SamoaResponse::data_block_length is appropriately updated.
     */
    void add_response_data_block(const core::buffer_regions_t &);

    /*!
     * \brief Adds the (byte) iteration-range as a response datablock
     *
     * SamoaResponse::data_block_length is appropriately updated.
     */
    template<typename Iterator>
    void add_response_data_block(const Iterator & beg, const Iterator & end);

    /*!
     * \brief Writes the client_state's SamoaResponse and
     *  response datablocks to the client.
     *
     * Postcondition note: After invoking flush_response(),
     *  it is an error to call add_response_data_block(), or to
     *  mutate the SamoaResponse
     *
     * @param guard Smart-ptr to maintain request::state reference
     */
    void flush_response(const state_ptr_t & guard);

    /*!
     * \brief Helper for sending an error response to the client
     *
     * Clears any state set in SamoaResponse, and arranges for
     *  an error response to be delieved to the client.
     *
     * Postcondition: same requirements as flush_response()
     *
     * @param guard Smart-ptr to maintain request::state reference
     * @param err_code Code to set on response error
     * @param err_msg Accompanying message
     */
    void send_error(const state_ptr_t & guard, unsigned err_code,
        const std::string & err_msg);

    /*!
     * \brief Helper for sending an error response to the client
     *
     * Clears any state set in SamoaResponse, and arranges for
     *  an error response to be delivered to the client.
     *
     * Postcondition: same requirements as flush_response()
     *
     * @param guard Smart-ptr to maintain request::state reference
     * @param err_code Code to set on response error
     * @param err_msg boost error code, which will be converted to a message
     */
    void send_error(const state_ptr_t & guard, unsigned err_code,
        const boost::system::error_code & err_msg);

    void load_client_state(const server::client_ptr_t & client);

    void reset_client_state();

private:

    friend class samoa::server::client;

    void on_response(server::client::response_interface,
        const state_ptr_t & guard);

    server::client_ptr_t _client;

    spb::SamoaRequest   _samoa_request;
    spb::SamoaResponse _samoa_response;

    std::vector<core::buffer_regions_t> _request_data_blocks;
    core::const_buffer_regions_t _response_data;

    bool _flush_response_called;
    core::buffer_ring _w_ring;
};

}
}

#include "samoa/request/client_state.impl.hpp"

#endif

