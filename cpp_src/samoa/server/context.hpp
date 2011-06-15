#ifndef SAMOA_SERVER_CONTEXT_HPP
#define SAMOA_SERVER_CONTEXT_HPP

#include "samoa/core/fwd.hpp"
#include "samoa/server/fwd.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include "samoa/core/uuid.hpp"
#include "samoa/spinlock.hpp"
#include <boost/function.hpp>
#include <string>

namespace samoa {
namespace server {

namespace spb = samoa::core::protobuf;

class context :
    public boost::enable_shared_from_this<context>
{
public:

    typedef context_ptr_t ptr_t;

    context(const spb::ClusterState &);

    const core::uuid & get_server_uuid() const
    { return _uuid; }

    const std::string & get_server_hostname() const
    { return _hostname; }

    unsigned short get_server_port() const
    { return _port; }

    cluster_state_ptr_t get_cluster_state() const;

    typedef boost::function<bool (spb::ClusterState &)
        > cluster_state_callback_t;

    //! Begins a cluster state transaction
    /*!
        A cluster state transaction is any operation which mutates the local
        server's view of the Samoa cluster. Such mutations can occur frequently,
        so responsibility of representation of the cluster shape is delegated
        to an (immutable) cluster_state class instance.

        As cluster_state is immutable, server operations can ensure a consistent
        view of the the cluster by referencing the current instance at operation
        start.

        Modifications of cluster state require transactional semantics,
        and enforce the classic ACID properties:

        Atomicity - a cluster_state_callback_t operates over a spb::ClusterState
          _copy_, and returns only when the mutation has been fully applied, or
          is to be discarded.

        Consistency - a runtime cluster_state instance is constructed
          from a mutated description before the transaction is commited.
          Invariants are checked by the constructor of cluster_state (and
          descendents).

        Isolation - callbacks to cluster_state_callback_t are serialized
          by cluster_state_transaction

        Durability - immediately before 'commiting' the transaction, the
          protobuf ClusterState description is written to disk.

        \sa cluster_state
    */
    void cluster_state_transaction(
        const cluster_state_callback_t &);

private:

    void on_cluster_state_transaction(const cluster_state_callback_t &);

    const core::uuid  _uuid;
    const std::string _hostname;
    const unsigned short _port;

    const core::proactor_ptr_t   _proactor;
    const core::io_service_ptr_t _io_srv;

    mutable spinlock _cluster_state_lock;
    cluster_state_ptr_t _cluster_state;
};

}
}

#endif

