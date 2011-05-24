
import sqlalchemy.sql.expression as sql_exp

from samoa.core import UUID
from samoa.persistence import DataType
import samoa.model
import samoa.core.protobuf

import peer_set
import table_set
from local_partition import LocalPartition

class ClusterState(object):
    """
    Represents an immutable, atomic snapshot of the cluster shape and
    configuration at a point in time.

    Samoa cluster configuration can change at any time, but on each change
    a new ClusterState, as well as PeerSet & TableSet instances are built.

    The previous ClusterState remains useable until no references remain.

    Thus, to guarentee a consistent view of the cluster configuration,
    operations should reference the current ClusterState at operation start,
    and use that reference for the operation duration.

    Particularly long-running operations should periodically release and
    re-aquire a ClusterState reference, checking that the operation still
    makes sense. 
    """

    def __init__(self, context, session, prev_state):

        self._peer_set = peer_set.PeerSet(context, session,
            prev_state and prev_state.get_peer_set())

        self._table_set = table_set.TableSet(context, session,
            prev_state and prev_state.get_table_set())

        self._cache_proto_state(context)

    def get_peer_set(self):
        return self._peer_set

    def get_table_set(self):
        return self._table_set

    def get_table(self, table_uuid):
        return self._table_set.get_table(table_uuid)

    def get_table_by_name(self, table_name):
        return self._table_set.get_table_by_name(table_name)

    @classmethod
    def update_from_peer(cls, context, proto_state):
        yield _ClusterState_Updater(context).update_from(proto_state)
        yield

    def build_proto_cluster_state(self, proto_state):

        proto_state.CopyFrom(self._cached_proto_state)

        for proto_peer in proto_state.peer:

            # add informational, current peer state
            server = self._peer_set.get_server(
                UUID.from_hex_str(proto_peer.uuid))

            if server:
                proto_peer.connected = True
                proto_peer.queue_size = server.get_queue_size()
                proto_peer.latency_ms = server.get_latency_ms()
            else:
                proto_peer.connected = False

    def _cache_proto_state(self, context):

        cluster_state = samoa.core.protobuf.ClusterState()

        cluster_state.local_uuid = context.get_server_uuid().to_hex_str()
        cluster_state.local_hostname = context.get_server_hostname()
        cluster_state.local_port = context.get_server_port()

        # enumerate peers
        for peer in self._peer_set.get_peers():

            proto_peer = cluster_state.add_peer()

            proto_peer.uuid = peer.uuid.to_hex_str()
            proto_peer.hostname = peer.hostname
            proto_peer.port = peer.port

        # enumerate live tables
        for table in self._table_set.get_tables():

            proto_table = cluster_state.add_table()

            # add cluster-shared table state
            proto_table.uuid = table.get_uuid().to_hex_str()
            proto_table.name = table.get_name()
            proto_table.data_type = table.get_data_type().name
            proto_table.replication_factor = table.get_replication_factor()

            # enumerate live partitions
            for part in table.get_ring():

                proto_part = proto_table.add_partition()

                proto_part.uuid = part.get_uuid().to_hex_str()
                proto_part.server_uuid = part.get_server_uuid().to_hex_str()
                proto_part.ring_position = part.get_ring_position()
                proto_part.consistent_range_begin = \
                    part.get_consistent_range_begin()
                proto_part.consistent_range_end = \
                    part.get_consistent_range_end()
                proto_part.lamport_ts = part.get_lamport_ts()
                #proto_part.storage_path = part.storage_path
                #proto_part.storage_size = part.storage_size
                #proto_part.index_size = part.index_size

            # enumerate dropped partitions
            for part_uuid in table.get_dropped_partition_uuids():

                proto_part = proto_table.add_partition()

                proto_part.uuid = part_uuid.to_hex_str()
                proto_part.dropped = True

        # enumerate dropped tables
        for table_uuid in self._table_set.get_dropped_table_uuids():

            proto_table = cluster_state.add_table()

            proto_table.uuid = table_uuid.to_hex_str()
            proto_table.dropped = True

        self._cached_proto_state = cluster_state


class ProtobufUpdator(object):

    def __init__(self, proto_state):
        self.proto_state = proto_state
        self.new_peers = {}

    def update(self, context, session):

        self.context = context
        self.session = session
        self.log = context.log

        self.server_uuid = context.get_server_uuid()

        self.prev_cluster_state = context.get_cluster_state()
        self.prev_table_set = self.prev_cluster_state.get_table_set()
        self.prev_peer_set = self.prev_cluster_state.get_peer_set()

        # First, identify remote peers which are not locally known.
        # Adding unknown peers is deferred until it's known
        #  whether they're required by a locally-tracked partition
        for proto_peer in self.proto_state.peer:
            peer_uuid = UUID.from_hex_str(proto_peer.uuid)

            if not self.prev_peer_set.get_peer(peer_uuid):
                self.new_peers[peer_uuid] = (
                    proto_peer.hostname, proto_peer.port)

        # The originator of the ClusterState message is also a potential peer
        peer_uuid = UUID.from_hex_str(self.proto_state.local_uuid)

        if not self.prev_peer_set.get_peer(peer_uuid):
            self.new_peers[peer_uuid] = (
                self.proto_state.local_hostname, self.proto_state.local_port)

        # Merge tables & table partitions
        dirty = self._merge_table_set(self.proto_state.table)

        if not dirty:
            # no change to cluster state
            yield False, False

        self.session.flush()

        for peer_uuid in self.new_peers.keys():
            self.log.info('discarding remote peer %r (not used)' % peer_uuid) 

        # Delete known peers whom we no longer care about
        for model in self.session.query(samoa.model.Server).filter(
            sql_exp.not_(samoa.model.Server.uuid.in_(
                self.session.query(samoa.model.Partition.server_uuid)))):

            self.log.info('dropping remote peer %r (%r, %r)' % (
                model.uuid, model.hostname, model.port))

            self.session.delete(model)

        yield True, False

    def _merge_table_set(self, proto_set):

        dirty = False

        for proto_table in proto_set:
            table_uuid = UUID.from_hex_str(proto_table.uuid)

            if self.prev_table_set.was_dropped(table_uuid):
                # locally dropped => ignore
                continue

            table = self.prev_table_set.get_table(table_uuid)

            if proto_table.dropped and not table:
                # remotely dropped but unknown => ignore
                continue

            if not table:
                # Table is unknown to us. Add it.
                model = samoa.model.Table(
                    uuid = table_uuid,
                    name = proto_table.name,
                    data_type = DataType.names[proto_table.data_type],
                    replication_factor = proto_table.replication_factor)
                self.session.add(model)

                # flush here, or we'll violate fk-constraints
                #  on inserting table partitions
                self.session.flush()
                dirty = True

                self.log.info('discovered remote table %r (%r)' % (
                    model.name, model.uuid))

            elif proto_table.dropped:
                # Locally-live table has been remotely dropped
                model = self.session.query(samoa.model.Table).filter_by(
                    uuid = table_uuid).one()

                model.dropped = True

                # Drop all table partitions as well
                self.session.query(samoa.model.Partition).filter_by(
                    table_uuid = table_uuid).update({'dropped': True})

                dirty = True
                self.log.info('remotely dropped table %r (%r)' % (
                    model.name, model.uuid))
                continue

            # merge partitions of the table
            if self._merge_table(table, proto_table):
                dirty = True

        return dirty

    def _merge_table(self, table, proto_table):

        dirty = False

        # First, look for modifications to locally-tracked partitions.
        # Also identify live remote partitions not currently tracked.
        unknown_partitions = {}

        for proto_part in proto_table.partition:
            part_uuid = UUID.from_hex_str(proto_part.uuid)
            server_uuid = UUID.from_hex_str(proto_part.server_uuid) 

            if table and table.was_dropped(part_uuid):
                # locally dropped => ignore
                continue

            part = table and table.get_partition(part_uuid)

            if proto_part.dropped and not part:
                # remotely dropped but unknown => ignore
                continue

            if not part:
                # Partition is unkown to us. We may need to add it later.
                assert server_uuid != self.server_uuid
                key = (proto_part.ring_position, part_uuid, False)
                unknown_partitions[key] = proto_part

            elif proto_part.dropped:
                # Locally-live partition has been remotely dropped
                model = session.query(samoa.model.Partition).filter_by(
                    uuid = part_uuid).one()

                model.dropped = True
                dirty = True

                self.log.info('remotely dropped partition %r (table %r)' % (
                    model.uuid, model.table_uuid))

            elif proto_part.lamport_ts > part.get_lamport_ts():
                # Remote partition has updated metadata
                model = session.query(samoa.model.Partition).filter_by(
                    uuid = part_uuid).one()

                model.consistent_range_begin = proto_part.consistent_range_begin
                model.consistent_range_end = proto_part.consistent_range_end
                model.lamport_ts = proto_part.lamport_ts

        # currently-tracked partitions, as:
        #  (ring-position, partition uuid, is-local)
        tracked_partitions = []

        if table and not dirty:
            # Build from the runtime table
            tracked_partitions = [
                (p.get_ring_position(), p.get_uuid(), isinstance(p, LocalPartition)) \
                for p in table.get_ring()]
        else:
            # One or more local partitions has been modified
            #  We need to rebuild from the database
            table_uuid = UUID.from_hex_str(proto_table.uuid)

            for model in self.session.query(samoa.model.Partition).filter_by(
                table_uuid = table_uuid):

                if model.dropped:
                    continue

                is_local = model.server_uuid == self.server_uuid

                tracked_partitions.append(
                    (model.ring_position, model.uuid, is_local))

            tracked_partitions.sort()

        # Determine whether any unknown partitions should be locally tracked,
        #  and what currently tracked partitions should be dropped
        insertions, deletions = samoa.model.Table.compute_ring_update(
            tracked_partitions, sorted(unknown_partitions.keys()))

        if not insertions and not deletions:
            # No further changes to be made
            return dirty

        for ring_pos, part_uuid, is_local in insertions:
            assert not is_local
            # New remote partition to track
            proto_part = unknown_partitions[(ring_pos, part_uuid, False)]

            table_uuid = UUID.from_hex_str(proto_table.uuid)
            server_uuid = UUID.from_hex_str(proto_part.server_uuid) 

            if server_uuid in self.new_peers:
                # We now know the peer ought to be locally tracked,
                #  as this partition requires it
                self._add_peer(server_uuid)

            model = samoa.model.Partition(
                uuid = part_uuid,
                table_uuid = table_uuid,
                server_uuid = server_uuid,
                ring_position = proto_part.ring_position,
                consistent_range_begin = proto_part.consistent_range_begin,
                consistent_range_end = proto_part.consistent_range_end,
                lamport_ts = proto_part.lamport_ts,
                storage_path = proto_part.storage_path,
                storage_size = proto_part.storage_size,
                index_size = proto_part.index_size)
            self.session.add(model)

            self.log.info('discovered remote partition %r (table %r)' % (
                    model.uuid, model.table_uuid))

        for ring_pos, part_uuid, is_local in deletions:
            assert not is_local

            self.session.query(samoa.model.Partition).filter_by(
                uuid = part_uuid).delete()

            self.log.info('dropped remote partition %r from local ring' % part_uuid)

        return True

    def _add_peer(self, peer_uuid):

        hostname, port = self.new_peers[peer_uuid]
        del self.new_peers[peer_uuid]

        model = samoa.model.Server(
            uuid = peer_uuid,
            hostname = hostname,
            port = port)

        self.session.add(model)
        self.session.flush()

        self.log.info('added new peer %r (%r, %r)' % (
            model.uuid, model.hostname, model.port))

