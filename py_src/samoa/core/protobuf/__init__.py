
import bisect

from _samoa import *
del _samoa

from samoa.core.uuid import UUID

def add_peer(state, uuid):
    assert isinstance(state, ClusterState)

    peer = state.add_peer()
    peer.set_uuid(uuid.to_hex())

    # re-establish sorted invariant by bubbling into place
    keys = [t.uuid for t in state.peer]
    ind = bisect.bisect_left(keys[:-1], keys[-1])

    for ind in xrange(len(state.peer) - 1, ind, -1):
        state.peer.SwapElements(ind, ind - 1)

    return peer

def add_table(state, uuid):
    assert isinstance(state, ClusterState)

    tbl = state.add_table()
    tbl.set_uuid(uuid.to_hex())

    # re-establish sorted invariant by bubbling into place
    keys = [t.uuid for t in state.table]
    ind = bisect.bisect_left(keys[:-1], keys[-1])

    for ind in xrange(len(state.table) - 1, ind, -1):
        state.table.SwapElements(ind, ind - 1)

    return tbl

def add_partition(state, uuid, ring_position):
    assert isinstance(state, ClusterState_Table)

    part = state.add_partition()
    part.set_uuid(uuid.to_hex())
    part.set_ring_position(ring_position)
    
    # re-establish sorted invariant by bubbling into place
    keys = [(p.ring_position, p.uuid) for p in state.partition]
    ind = bisect.bisect_left(keys[:-1], keys[-1])

    for ind in xrange(len(state.partition) - 1, ind, -1):
        state.partition.SwapElements(ind, ind - 1)

    return part

def find_peer(state, uuid):
    assert isinstance(state, ClusterState)

    keys = [UUID(t.uuid) for t in state.peer]
    ind = bisect.bisect_left(keys, uuid)

    if ind == len(keys) or UUID(keys[ind]) != uuid:
        return None

    return state.peer[ind]

def find_table(state, uuid):
    assert isinstance(state, ClusterState)

    keys = [UUID(t.uuid) for t in state.table]
    ind = bisect.bisect_left(keys, uuid)

    if ind == len(keys) or UUID(keys[ind]) != uuid:
        return None

    return state.table[ind]

def find_table_by_name(state, name):
    assert isinstance(state, ClusterState)

    for table in state.table:
        if not table.dropped and table.name == name:
            return table

    return None

def find_partition(state, uuid):
    assert isinstance(state, ClusterState_Table)

    for part in state.partition:
        if UUID(part.uuid) == uuid:
            return part

    return None

