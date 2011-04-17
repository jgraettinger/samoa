
class LocalPartition(object):

    is_local = True

    """

    locate_record(self, key, allow_blocking) -> record

    allocate_record(self, key, value_length_estimate) -> record


    """

    def __init__(self, model, table, prev_partition):
        self.uuid = model.uuid
        assert not model.dropped

        self.server_uuid = model.server_uuid
        self.ring_position = model.ring_position
        self.consistent_range_begin = model.consistent_range_begin
        self.consistent_range_end = model.consistent_range_end
        self.lamport_ts = model.lamport_ts
        self.storage_path = model.storage_path
        self.storage_size = model.storage_size
        self.index_size = model.index_size




#        self._rolling_hash = MappedRollingHash.open(
#            self.storage_path, self.storage_size, self.index_size)

"""
    def _boot_coro(self, table, local_ring):

        # local_ring is [-r + 1, -r + 2, ..., 0, 1, 2, ... r - 1]
        #  local_ring[r-1] == 0 == <this partition>
        #
        # local_ring[0].ring_position ... local_ring[r].ring_position is the
        #  area of responsibility for this node:

        repl = table.replication_factor
        local_offset = repl - 1

        repl_low_pos = local_ring[0].ring_position
        repl_high_pos = local_ring[repl].ring_position

        for index in range(local_offset - 1, -1, -1):
            # iterate from this partition's closest predecessor,
            #  to this partition's farthest predecessor

            # this partition's ring areas of authoritative knowledge
            start_pos = repl_low_pos
            end_pos = local_ring[index + 1].ring_position

            try:
                local_ring[index].iterate(start_pos, end_pos)
                repl_low_pos = end_pos
                break
            except:
                # try next predecessor
                pass

        for index in range(local_offset + 1, 2 * repl):
            # iterate from this partition's first sucessor,
            # to this partition's furthest successor

            try:
                local_ring[index].iterate(repl_low_pos, repl_high_pos)
                break
            except:
                # try next successor
                pass

        self.online = True
"""

