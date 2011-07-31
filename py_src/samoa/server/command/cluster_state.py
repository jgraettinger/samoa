
import logging
import functools
import getty

from samoa.core import protobuf
from samoa.server.command_handler import CommandHandler

class ClusterStateHandler(CommandHandler):

    @getty.requires(log = logging.Logger)
    def __init__(self, log):
        CommandHandler.__init__(self)
        self.log = log

    def _transaction(self, client, local_state):
        cluster_state = client.get_context().get_cluster_state()
        peer_state = client.get_request().cluster_state

        return cluster_state.merge_cluster_state(
            peer_state, local_state)

    def handle(self, client):

        if client.get_request().has_cluster_state():

            # begin a transaction to merge remote state with our own
            try:
                yield client.get_context().cluster_state_transaction(
                    functools.partial(self._transaction, client))
            except RuntimeError, e:
                self.log.exception(e)
                client.send_error(406, e.message)
                yield 

        cluster_state = client.get_context().get_cluster_state()
        client.get_response().mutable_cluster_state().CopyFrom(
            cluster_state.get_protobuf_description())

        client.finish_response()
        yield

