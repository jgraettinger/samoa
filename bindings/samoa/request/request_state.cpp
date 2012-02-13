
#include <boost/python.hpp>
#include "samoa/request/request_state.hpp"
#include "samoa/server/context.hpp"
#include "samoa/server/cluster_state.hpp"
#include "samoa/server/table_set.hpp"
#include "samoa/server/peer_set.hpp"
#include "samoa/server/table.hpp"
#include "samoa/server/local_partition.hpp"
#include "samoa/server/partition.hpp"
#include "samoa/server/client.hpp"

namespace samoa {
namespace request {

namespace bpl = boost::python;

// this is... annoying. request::state selectively exposes
// methods of it's base classes via the 'using' keyword,
// but pointers taken of these member functions retain the
// base type in their type signature. boost::python then
// attempts to cast request::state to it's private base,
// and blows up.
//
// Here we define wrappers for each method. It'd be nice to
// come up with a metaprogramming solution to generate these
// wrappers.
//
// An advantage is it removes const-ref qualifiers on return
// types and does away with bpl::return_value_policy boilerplate

core::io_service_ptr_t py_get_io_service(state & s)
{ return s.get_io_service(); }

server::context_ptr_t py_get_context(state & s)
{ return s.get_context(); }

server::cluster_state_ptr_t py_get_cluster_state(state & s)
{ return s.get_cluster_state(); }

server::peer_set_ptr_t py_get_peer_set(state & s)
{ return s.get_peer_set(); }

server::table_set_ptr_t py_get_table_set(state & s)
{ return s.get_table_set(); }

server::client_ptr_t py_get_client(state & s)
{ return s.get_client(); }

const spb::SamoaRequest & py_get_samoa_request(state & s)
{ return s.get_samoa_request(); }

bpl::list py_get_request_data_blocks(state & s)
{
    bpl::list out;

    for(auto it = s.get_request_data_blocks().begin();
        it != s.get_request_data_blocks().end(); ++it)
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

spb::SamoaResponse & py_get_samoa_response(state & s)
{ return s.get_samoa_response(); }

void py_add_response_data_block(state & s, const bpl::str & py_str)
{
    char * cbuf;
    Py_ssize_t length;

    if(PyString_AsStringAndSize(py_str.ptr(), &cbuf, &length) == -1)
        bpl::throw_error_already_set();

    s.add_response_data_block(cbuf, cbuf + length);
}

void py_flush_response(state & s)
{ s.flush_response(); }

void py_send_error(state & s, unsigned code, const std::string & msg)
{ s.send_error(code, msg); }

core::uuid py_get_table_uuid(state & s)
{ return s.get_table_uuid(); }

void py_set_table_uuid(state & s, const core::uuid & u)
{ s.set_table_uuid(u); }

std::string py_get_table_name(state & s)
{ return s.get_table_name(); }

server::table_ptr_t py_get_table(state & s)
{ return s.get_table(); }

std::string py_get_key(state & s)
{ return s.get_key(); }

void py_set_key(state & s, std::string key)
{ s.set_key(std::move(key)); }

uint64_t py_get_ring_position(state & s)
{ return s.get_ring_position(); }

bool py_has_primary_partition_uuid(state & s)
{ return s.has_primary_partition_uuid(); }

core::uuid py_get_primary_partition_uuid(state & s)
{ return s.get_primary_partition_uuid(); }

void py_set_primary_partition_uuid(state & s, const core::uuid & u)
{ s.set_primary_partition_uuid(u); }

server::local_partition_ptr_t py_get_primary_partition(state & s)
{ return s.get_primary_partition(); }

bool py_has_peer_partition_uuids(state & s)
{ return s.has_peer_partition_uuids(); }

bpl::list py_get_peer_partition_uuids(state & s)
{
    bpl::list out;

    for(auto it = s.get_peer_partition_uuids().begin();
        it != s.get_peer_partition_uuids().end(); ++it)
    {
        out.append(*it);
    }
    return out;
}

bpl::list py_get_peer_partitions(state & s)
{
    bpl::list out;

    for(auto it = s.get_peer_partitions().begin();
        it != s.get_peer_partitions().end(); ++it)
    {
        out.append(*it);
    }
    return out;
}

spb::PersistedRecord & py_get_local_record(state & s)
{ return s.get_local_record(); }

spb::PersistedRecord & py_get_remote_record(state & s)
{ return s.get_remote_record(); }

unsigned py_get_quorum_count(state & s)
{ return s.get_quorum_count(); }

unsigned py_get_peer_success_count(state & s)
{ return s.get_peer_success_count(); }

unsigned py_get_peer_failure_count(state & s)
{ return s.get_peer_failure_count(); }

bool py_peer_replication_failure(state & s)
{ return s.peer_replication_failure(); }

bool py_peer_replication_success(state & s)
{ return s.peer_replication_success(); }

bool py_is_replication_finished(state & s)
{ return s.is_replication_finished(); }

core::murmur_hash::checksum_t py_get_replication_checksum(state & s)
{ return s.get_replication_checksum(); }

void py_set_replication_checksum(state & s,
    const core::murmur_hash::checksum_t & checksum)
{ return s.set_replication_checksum(checksum); }

void make_state_bindings()
{
    bpl::class_<state, state::ptr_t, boost::noncopyable>(
            "RequestState", bpl::init<>())

        // io_service_state
        .def("get_io_service", &py_get_io_service)

        // context_state
        .def("get_context", &py_get_context)
        .def("get_cluster_state", &py_get_cluster_state)
        .def("get_peer_set", &py_get_peer_set)
        .def("get_table_set", &py_get_table_set)

        // client_state
        .def("get_client", &py_get_client)
        .def("get_samoa_request", &py_get_samoa_request,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("get_request_data_blocks", &py_get_request_data_blocks)
        .def("get_samoa_response", &py_get_samoa_response,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("add_response_data_block", &py_add_response_data_block)
        .def("flush_response", &py_flush_response)
        .def("send_error", &py_send_error,
            (bpl::arg("code"), bpl::arg("message")))

        // table_state
        .def("load_table_state", &state::load_table_state)

        .def("get_table_uuid", &py_get_table_uuid)
        .def("set_table_uuid", &py_set_table_uuid)
        .def("get_table_name", &py_get_table_name)
        .def("get_table", &py_get_table)

        // route_state
        .def("load_route_state", &state::load_route_state)

        .def("get_key", &py_get_key)
        .def("set_key", &py_set_key)
        .def("get_ring_position", &py_get_ring_position)
        .def("has_primary_partition_uuid", &py_has_primary_partition_uuid)
        .def("get_primary_partition_uuid", &py_get_primary_partition_uuid)
        .def("set_primary_partition_uuid", &py_set_primary_partition_uuid)
        .def("get_primary_partition", &py_get_primary_partition)
        .def("has_peer_partition_uuids", &py_has_peer_partition_uuids)
        .def("get_peer_partition_uuids", &py_get_peer_partition_uuids)
        .def("get_peer_partitions", &py_get_peer_partitions)

        // record_state
        .def("get_local_record", &py_get_local_record,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("get_remote_record", &py_get_remote_record,
            bpl::return_value_policy<bpl::reference_existing_object>())

        // replication_state
        .def("load_replication_state", &state::load_replication_state)

        .def("get_quorum_count", &py_get_quorum_count)
        .def("get_peer_success_count", &py_get_peer_success_count)
        .def("get_peer_failure_count", &py_get_peer_failure_count)
        .def("peer_replication_failure", &py_peer_replication_failure)
        .def("peer_replication_success", &py_peer_replication_success)
        .def("is_replication_finished", &py_is_replication_finished)
        .def("get_replication_checksum", &py_get_replication_checksum)
        .def("set_replication_checksum", &py_set_replication_checksum)

        .def("parse_samoa_request", &state::parse_samoa_request)
        .def("reset_state", &state::reset_state)
        ;
}

}
}

