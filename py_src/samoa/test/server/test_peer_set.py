
import getty
import unittest

from samoa.core.protobuf import ClusterState, CommandType
from samoa.core.uuid import UUID
from samoa.core.proactor import Proactor
from samoa.client.server import Server
from samoa.server.peer_set import PeerSet
from samoa.server.table_set import TableSet
from samoa.server.listener import Listener
from samoa.server.command_handler import CommandHandler

from samoa.test.module import TestModule
from samoa.test.cluster_state_fixture import ClusterStateFixture


class TestPeerSet(unittest.TestCase):

    def test_ctor_edge_cases(self):

        fixture = ClusterStateFixture()

        fixture.add_peer()
        fixture.add_peer()

        # null hypothesis - should build normally
        peer_set = PeerSet(fixture.state, None)

        # invalid peer order
        tst_state = ClusterState(fixture.state)
        tst_state.peer.SwapElements(0, 1)

        with self.assertRaisesRegexp(RuntimeError, 'assertion_failure'):
            PeerSet(tst_state, None)

        # duplicate peer UUID
        tst_state = ClusterState(fixture.state)
        tst_state.peer[0].set_uuid(tst_state.peer[1].uuid)

        with self.assertRaisesRegexp(RuntimeError, 'assertion_failure'):
            PeerSet(tst_state, None)

    def test_merge_edge_cases(self):

        fixture = ClusterStateFixture()
        table = fixture.add_table().uuid

        pgen = fixture.clone_peer()

        p1 = pgen.add_peer().uuid
        p2 = pgen.add_peer().uuid

        # p1 is referenced, p2 is not
        pgen.add_remote_partition(table, server_uuid = p1)

        peer_set = PeerSet(fixture.state, None)

        # null hypothesis - should merge normally
        peer_set.merge_peer_set(pgen.state,
            ClusterState(fixture.state))

        # invalid peer order
        tst_state = ClusterState(pgen.state)
        tst_state.peer.SwapElements(0, 1)

        with self.assertRaisesRegexp(RuntimeError, 'assertion_failure'):
            peer_set.merge_peer_set(tst_state,
                ClusterState(fixture.state))

        # duplicate peer UUID
        tst_state = ClusterState(pgen.state)
        tst_state.peer[1].set_uuid(tst_state.peer[0].uuid)

        with self.assertRaisesRegexp(RuntimeError, 'assertion_failure'):
            peer_set.merge_peer_set(tst_state,
                ClusterState(fixture.state))

    def test_merge_extended(self):

        fixture = ClusterStateFixture()
        table = fixture.add_table().uuid

        # common set of known peers
        p1 = fixture.add_peer().uuid
        p2 = fixture.add_peer().uuid
        p3 = fixture.add_peer().uuid

        # create remote partitions referencing all peers
        part1 = fixture.add_remote_partition(table,
            server_uuid = p1).uuid
        part2 = fixture.add_remote_partition(table,
            server_uuid = p2).uuid
        part3 = fixture.add_remote_partition(table,
            server_uuid = p3).uuid

        pgen = fixture.clone_peer()

        # locally drop part1
        part = fixture.get_partition(table, part1)
        rpos = part.ring_position

        part.Clear()
        part.set_uuid(part1)
        part.set_ring_position(rpos)
        part.set_dropped(True)

        # peer drops part2 & part 3
        part = pgen.get_partition(table, part2)
        rpos = part.ring_position

        part.Clear()
        part.set_uuid(part2)
        part.set_ring_position(rpos)
        part.set_dropped(True)

        part = pgen.get_partition(table, part3)
        rpos = part.ring_position

        part.Clear()
        part.set_uuid(part3)
        part.set_ring_position(rpos)
        part.set_dropped(True)

        # locally set p3 as a 'seed' peer
        fixture.get_peer(p3).set_seed(True)

        # additional peers known locally
        p4 = fixture.add_peer().uuid
        p5 = fixture.add_peer().uuid
        fixture.add_remote_partition(table, server_uuid = p4)

        # additional peers known remotely
        p6 = pgen.add_peer().uuid
        p7 = pgen.add_peer().uuid
        pgen.add_remote_partition(table, server_uuid = p7)

        # peer has a local partition => peer itself should be tracked
        pgen.add_local_partition(table)

        peer_set = PeerSet(fixture.state, None)
        table_set = TableSet(fixture.state, None)

        # p1 is not referenced, but still known
        peer_set.get_server_hostname(UUID(p1))

        # p2 is locally referenced & known
        peer_set.get_server_hostname(UUID(p2))

        # p3 is locally known / referenced
        peer_set.get_server_hostname(UUID(p3))

        # p4 is locally referenced & known
        peer_set.get_server_hostname(UUID(p4))

        # p5 is locally not referenced, but still known
        peer_set.get_server_hostname(UUID(p5))

        # p6 isn't yet known
        with self.assertRaisesRegexp(RuntimeError, "<assertion_failure>"):
            peer_set.get_server_hostname(UUID(p6))

        # p7 isn't known
        with self.assertRaisesRegexp(RuntimeError, "<assertion_failure>"):
            peer_set.get_server_hostname(UUID(p7))

        # peer itself isn't yet known
        with self.assertRaisesRegexp(RuntimeError, "<assertion_failure>"):
            peer_set.get_server_hostname(UUID(pgen.state.local_uuid))

        # MERGE from peer, & rebuild peer_set / table_set
        out = ClusterState(fixture.state)
        self.assertTrue(table_set.merge_table_set(pgen.state, out))
        peer_set.merge_peer_set(pgen.state, out)

        peer_set = PeerSet(out, peer_set)
        table_set = TableSet(out, table_set)

        # p1 is not kept (partition locally dropped)
        with self.assertRaisesRegexp(RuntimeError, "<assertion_failure>"):
            peer_set.get_server_hostname(UUID(p1))

        # p2 is not kept (partition remotely dropped)
        with self.assertRaisesRegexp(RuntimeError, "<assertion_failure>"):
            peer_set.get_server_hostname(UUID(p2))

        # p3 is still kept as seed, despite parition being remotely dropped
        peer_set.get_server_hostname(UUID(p3))

        # p4 is still known / referenced
        peer_set.get_server_hostname(UUID(p4))

        # p5 is not kept (not referenced)
        with self.assertRaisesRegexp(RuntimeError, "<assertion_failure>"):
            peer_set.get_server_hostname(UUID(p5))

        # p6 was never added (not referenced)
        with self.assertRaisesRegexp(RuntimeError, "<assertion_failure>"):
            peer_set.get_server_hostname(UUID(p6))

        # p7 is now known
        peer_set.get_server_hostname(UUID(p7))

        # peer itself is now known
        peer_set.get_server_hostname(UUID(pgen.state.local_uuid))

        # no further changes detected from peer
        out2 = ClusterState(out)
        self.assertFalse(table_set.merge_table_set(pgen.state, out2))
        peer_set.merge_peer_set(pgen.state, out2)

        self.assertEquals(out.SerializeToText(),
            out2.SerializeToText())

    def test_forwarding(self):

        injector = TestModule().configure(getty.Injector())
        fixture = injector.get_instance(ClusterStateFixture)

        listener = injector.get_instance(Listener)
        context = listener.get_context()

        # a proxy server which will forward requests
        proxy_injector = TestModule().configure(getty.Injector())
        proxy_fixture = proxy_injector.get_instance(ClusterStateFixture)

        proxy_fixture.add_peer(context.get_server_uuid(),
            port = context.get_server_port(), seed = True)

        proxy_listener = proxy_injector.get_instance(Listener)
        proxy_context = proxy_listener.get_context()

        class ProxyHandler(CommandHandler):
            def handle(self_inner, client):
                cluster_state = client.get_context().get_cluster_state()

                cluster_state.get_peer_set().forward_request(
                    client, context.get_server_uuid())

        class MainHandler(CommandHandler):
            def handle(self_inner, client):

                # request payload should have been properly forwarded
                self.assertEquals(['request payload'],
                    client.get_request_data_blocks())

                # send two response payloads back
                payload1 = 'response payload'
                payload2 = 'extended response payload'

                client.get_response().add_data_block_length(len(payload1))
                client.get_response().add_data_block_length(len(payload2))

                client.start_response()
                client.write_interface().queue_write(payload1)
                client.write_interface().queue_write(payload2)
                client.finish_response()

        # set these handlers up on an unused protocol type
        proxy_listener.get_protocol().set_command_handler(CommandType.TEST,
            ProxyHandler())

        listener.get_protocol().set_command_handler(CommandType.TEST,
            MainHandler())

        def test():

            proxy_server = yield Server.connect_to(
                proxy_listener.get_address(), proxy_listener.get_port())

            request = yield proxy_server.schedule_request()

            # send a request to forward, with expected payload
            request.get_message().set_type(CommandType.TEST)
            payload = 'request payload'
            request.get_message().add_data_block_length(len(payload))
            request.start_request()
            request.write_interface().queue_write(payload)

            response = yield request.finish_request()

            self.assertEquals(response.get_response_data_blocks(),
                ['response payload', 'extended response payload'])

            response.finish_response()

            # cleanup
            context.get_tasklet_group().cancel_group()
            proxy_context.get_tasklet_group().cancel_group()
            yield

        Proactor.get_proactor().run_test(test)

