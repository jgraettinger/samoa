
#include <boost/python.hpp>
#include "samoa/server/request_state.hpp"
#include "samoa/server/context.hpp"
#include "samoa/server/client.hpp"
#include "samoa/server/local_partition.hpp"
#include "samoa/server/partition.hpp"
#include "samoa/server/table.hpp"
#include "samoa/server/peer_set.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include "samoa/core/proactor.hpp"
#include <string>


namespace samoa {
namespace server {

namespace bpl = boost::python;

bpl::list py_get_request_data_blocks(request_state & r)
{
    bpl::list out;

    for(auto it = r.get_request_data_blocks().begin();
        it != r.get_request_data_blocks().end(); ++it)
    {
        unsigned len = 0;
        for(auto it2 = it->begin(); it2 != it->end(); ++it2)
        {
            len += it2->size();
        }

        bpl::str buffer(bpl::handle<>(
            PyString_FromStringAndSize(0, len)));

        char * out_it = PyString_AS_STRING(buffer.ptr());
        for(auto it2 = it->begin(); it2 != it->end(); ++it2)
        {
            out_it = std::copy(it2->begin(), it2->end(), out_it);
        }
        out.append(buffer);
    }
    return out;
}

bpl::list py_get_partition_peers(const request_state & r)
{
    bpl::list out;

    for(auto it = r.get_partition_peers().begin();
        it != r.get_partition_peers().end(); ++it)
    {
        out.append(*it);
    }
    return out;
}

void py_add_response_data_block(request_state & r, const bpl::str & py_str)
{
    char * cbuf;
    Py_ssize_t length;

    if(PyString_AsStringAndSize(py_str.ptr(), &cbuf, &length) == -1)
        bpl::throw_error_already_set();

    r.add_response_data_block(cbuf, cbuf + length);
}

std::string py_repr(request_state & r)
{
    std::stringstream s;

    s << "request_state<";

    if(r.get_client())
    {
        s << r.get_client()->get_remote_address() << ":";
        s << r.get_client()->get_remote_port();
    } else {
        s << "no-client";
    }
    s << ">(" << r.get_samoa_request().ShortDebugString() << ")";

    return s.str();
}

void make_request_state_bindings()
{
    void (request_state::*send_client_error_ptr)(
        unsigned, const std::string &) = &request_state::send_client_error;

    bpl::class_<request_state, request_state::ptr_t, boost::noncopyable
        >("RequestState", bpl::init<const server::client::ptr_t &>())
        .def("__repr__", &py_repr)
        .def("load_from_samoa_request", &request_state::load_from_samoa_request)
        .def("get_samoa_request", &request_state::get_samoa_request,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("get_samoa_response", &request_state::get_samoa_response,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("get_request_data_blocks", &py_get_request_data_blocks)
        .def("get_client", &request_state::get_client,
            bpl::return_value_policy<bpl::copy_const_reference>())
        .def("get_context", &request_state::get_context,
            bpl::return_value_policy<bpl::copy_const_reference>())
        .def("get_peer_set", &request_state::get_peer_set,
            bpl::return_value_policy<bpl::copy_const_reference>())
        .def("get_table", &request_state::get_table,
            bpl::return_value_policy<bpl::copy_const_reference>())
        .def("get_key", &request_state::get_key,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("get_primary_partition", &request_state::get_primary_partition,
            bpl::return_value_policy<bpl::copy_const_reference>())
        .def("get_partition_peers", &py_get_partition_peers)
        .def("get_local_record", &request_state::get_local_record,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("get_remote_record", &request_state::get_remote_record,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("get_client_quorum", &request_state::get_client_quorum)
        .def("get_peer_error_count", &request_state::get_peer_error_count)
        .def("get_peer_success_count", &request_state::get_peer_success_count)
        .def("get_io_service", &request_state::get_io_service,
            bpl::return_value_policy<bpl::copy_const_reference>())
        .def("peer_replication_failure",
            &request_state::peer_replication_failure)
        .def("peer_replication_success",
            &request_state::peer_replication_success)
        .def("is_client_quorum_met", &request_state::is_client_quorum_met)
        .def("add_response_data_block", &py_add_response_data_block)
        .def("flush_client_response", &request_state::flush_client_response)
        .def("send_client_error", send_client_error_ptr,
            (bpl::arg("code"), bpl::arg("message")))
        ; 
}

}
}

