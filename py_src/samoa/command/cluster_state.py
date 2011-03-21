
import samoa.command

import samoa.model.server
import samoa.model.table
import samoa.model.partition

from samoa.core import protobuf

class ClusterState(samoa.command.Command):

    def __init__(self, local_cluster_state = None):
        samoa.command.Command.__init__(self)
        self._local_cluster_state = local_cluster_state

    def _write_request(self, request, server):

        request.type = protobuf.CommandType.CLUSTER_STATE
        if self._local_cluster_state:
            self._local_cluster_state.build_proto_cluster_state(
                request.mutable_cluster_state())

        yield

    def _read_response(self, response, server):

        resp_copy = protobuf.ClusterState()
        resp_copy.CopyFrom(response.cluster_state)
        yield resp_copy

    @classmethod
    def _handle(cls, client):

        context = client.get_context()
        request = client.get_request()
        response = client.get_response()

        # The cluster-state instance may change as a result
        #  of this call, so don't reference it directly
        if request.has_cluster_state():
            yield context.get_cluster_state().update_from_peer(
                client.get_context(), request.cluster_state)

        context.get_cluster_state().build_proto_cluster_state(
            client.get_response().mutable_cluster_state())
        yield

