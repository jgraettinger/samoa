
import time
import samoa.command.cluster_state
import samoa.server.cluster_state

import cluster_state

class Peer(object):

    polling_interval_ms = 300 * 1000

    def __init__(self, model, context, prev_peer):

        self.uuid = model.uuid
        self.hostname = model.hostname
        self.port = model.port

        if prev_peer:
            self._last_poll_ts = prev_peer._last_poll_ts
        else:
            self._last_poll_ts = 0

        interval = time.time() - self._last_poll_ts
        poll_delay_ms = int(self.polling_interval_ms - (interval * 1000))

        self._poll_timer = context.get_proactor().run_later(
            self.poll_cluster_state, max(poll_delay_ms, 0), (context,))

    def poll_cluster_state(self, context):

        # schedule next iteration
        self._last_poll_ts = time.time()
        self._poll_timer = context.get_proactor().run_later(
            self.poll_cluster_state, self.polling_interval_ms, (context,))

        cluster_state = context.get_cluster_state()

        # request peer cluster state, passing our own in the request
        cmd = samoa.command.cluster_state.ClusterState(
            local_cluster_state = cluster_state)
        peer_state = yield cmd.request_of(
            cluster_state.get_peer_set(), self.uuid)

        yield context.cluster_state_transaction(
            samoa.server.cluster_state.ProtobufUpdator(peer_state).update)
        yield

    def retire(self):
        self._poll_timer.cancel()

