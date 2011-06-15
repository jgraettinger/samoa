
import samoa.command

import samoa.model.server
import samoa.model.table
import samoa.model.partition
import samoa.server.cluster_state

from samoa.core import protobuf

class ClusterState(samoa.command.Command):

    def __init__(self, local_cluster_state = None):
        samoa.command.Command.__init__(self)
        self._local_cluster_state = local_cluster_state

    def _write_request(self, request, server):
        request.type = protobuf.CommandType.CLUSTER_STATE
        if self._local_cluster_state:
            request.mutable_cluster_state().CopyFrom(
                self._local_cluster_state.get_protobuf_description())
        yield

    def _read_response(self, response, server):
        resp_copy = protobuf.ClusterState()
        resp_copy.CopyFrom(response.cluster_state)
        yield resp_copy

class ClusterStateHandler(samoa.command.CommandHandler):

    def _handle(self, client):

        context = client.get_context()
        request = client.get_request()
        response = client.get_response()

        def transaction(new_state):
            cur_cluster_state = context.get_cluster_state()

            # merge peer state into current cluster state,
            #  and return whether the local state was updated
            return cur_cluster_state.merge_cluster_state(
                request.cluster_state, new_state)

        if request.has_cluster_state():

            # start a transaction to potentially update local
            #  cluster state with the peer's description
            yield context.cluster_state_transaction(
                samoa.server.cluster_state.ProtobufUpdator(
                    request.cluster_state).update)

        cur_cluster_state = context.get_cluster_state()
        client.get_response().CopyFrom(
            cur_cluster_state.get_protobuf_description())

        yield

