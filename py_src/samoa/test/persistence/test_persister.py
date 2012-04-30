
import unittest
import random

from samoa.core.protobuf import PersistedRecord
from samoa.core.proactor import Proactor
from samoa.datamodel.merge_func import MergeResult
from samoa.persistence.persister import Persister

# imports required for library loads
import samoa.request.request_state
import samoa.persistence.rolling_hash.hash_ring

from samoa.test.cluster_state_fixture import ClusterStateFixture

class TestPersister(unittest.TestCase):

    def test_put(self):
        def test():

            persister = Persister()
            persister.add_heap_hash_ring(1<<14, 10)

            ring = persister.layer(0)

            record = PersistedRecord()
            record.add_blob_value('bar')

            def not_called(local_record, remote_record):
                self.assertFalse(True)

            # Putting a new key succeeds w/o merge callback
            self.assertTrue(
                (yield persister.put(not_called, 'foo', record)))

            self.assertEquals('bar',
                (yield persister.get('foo')).blob_value[0])

            def checked_replace(local_record, remote_record):
                self.assertEquals(local_record.blob_value[0], 'bar')
                self.assertEquals(remote_record.blob_value[0], 'baz')

                local_record.CopyFrom(remote_record)

                return MergeResult(
                    local_was_updated = True,
                    remote_is_stale = False)

            expected_end_offset = ring.end_offset()

            # Putting an update invokes merge callback
            record.blob_value[0] = 'baz'
            self.assertTrue(
                (yield persister.put(checked_replace, 'foo', record)))

            # Merge callback updated local_record, and it was stored
            self.assertEquals('baz',
                (yield persister.get('foo')).blob_value[0])

            # The updated value was the same size => no new allocation
            self.assertEquals(ring.end_offset(), expected_end_offset)

            def checked_replace(local_record, remote_record):
                self.assertEquals(local_record.blob_value[0], 'baz')
                self.assertEquals(remote_record.blob_value[0],
                    'a much longer value')

                local_record.CopyFrom(remote_record)

                return MergeResult(
                    local_was_updated = True,
                    remote_is_stale = False)

            # Putting a larger update
            record.blob_value[0] = 'a much longer value'
            self.assertTrue(
                (yield persister.put(checked_replace, 'foo', record)))

            # Merge callback updated local_record, and it was stored
            self.assertEquals('a much longer value',
                (yield persister.get('foo')).blob_value[0])

            # Larger value required a re-allocation
            self.assertNotEquals(ring.end_offset(), expected_end_offset)

            def abort_merge_callback(local_record, remote_record):
                self.assertEquals(local_record.blob_value[0],
                    'a much longer value')
                self.assertEquals(remote_record.blob_value[0], 'bing')

                local_record.CopyFrom(remote_record)

                return MergeResult(
                    local_was_updated = False,
                    remote_is_stale = False)

            # A second update, which doesn't commit
            record.blob_value[0] = 'bing'
            self.assertTrue(
                (yield persister.put(abort_merge_callback, 'foo', record)))

            # Despite updating the local_record copy, merge callback
            #  returned that local wasn't updated => it wasn't stored
            self.assertEquals('a much longer value',
                (yield persister.get('foo')).blob_value[0])

            yield

        Proactor.get_proactor().run(test())

    def test_drop(self):
        def test():

            persister = Persister()
            persister.add_heap_hash_ring(1<<14, 10)

            record = PersistedRecord()
            record.add_blob_value('bar')

            def not_called():
                self.assertFalse(True)

            # Place a fixture value
            self.assertTrue(
                (yield persister.put(not_called, 'foo', record)))

            # Attempt to drop an unknown key is no-op, & yields False
            self.assertEquals(False,
                (yield persister.drop(not_called, 'bar')))

            def rollback(local_record):
                self.assertEquals(local_record.blob_value[0], 'bar')
                return False

            # Attempt to drop 'foo', but don't commit; returns False
            self.assertEquals(False,
                (yield persister.drop(rollback, 'foo')))

            # Key / value are still present
            self.assertEquals('bar',
                (yield persister.get('foo')).blob_value[0])

            def commit(local_record):
                self.assertEquals(local_record.blob_value[0], 'bar')
                return True

            # This time, record is dropped
            self.assertEquals(True,
                (yield persister.drop(commit, 'foo')))

            # Key / value are no longer present
            self.assertFalse((yield persister.get('foo')))
            yield        

        Proactor.get_proactor().run(test())

    def test_iteration(self):
        def test():

            persister = Persister()
            persister.add_heap_hash_ring(1<<14, 10)

            record = PersistedRecord()
            record.add_blob_value('')

            put_fixture_order = [
                ('foo', 'foo value'),
                ('bar', 'bar value'),
                ('baz', 'baz value'),
                # causes a re-allocation
                ('bar', 'a much longer bar value')]

            # place several fixture keys / values
            for key, value in put_fixture_order:
                record.blob_value[0] = value
                self.assertTrue(
                    (yield persister.put(self._replace_callback, key, record)))

            iteration_fixture_order = [
                ('foo', 'foo value'),
                ('baz', 'baz value'),
                ('bar', 'a much longer bar value')]

            def not_called():
                self.assertFalse(True)

            outer_ticket = persister.iteration_begin()

            # perform N ** 2 iteration steps, where for each step of the
            #  outer iterator, we perform a full 'inner' iteration

            for outer_key, outer_value in iteration_fixture_order:

                def outer_callback(element):
                    self.assertEquals(element.key(), outer_key)
                    element.parse_persisted_record(record)

                not_done = yield persister.iteration_next(
                    outer_callback, outer_ticket)
                self.assertEquals(record.blob_value[0], outer_value)
                self.assertTrue(not_done)

                # perform complete inner iteration
                inner_ticket = persister.iteration_begin()

                for inner_key, inner_value in iteration_fixture_order:

                    def inner_callback(element):
                        self.assertEquals(element.key(), inner_key)
                        element.parse_persisted_record(record)

                    not_done = yield persister.iteration_next(
                        inner_callback, inner_ticket)
                    self.assertEquals(record.blob_value[0], inner_value)
                    self.assertTrue(not_done)

                # check end-of-iteration
                not_done = yield persister.iteration_next(
                    not_called, inner_ticket)
                self.assertFalse(not_done)

            # check end-of-iteration
            not_done = yield persister.iteration_next(
                not_called, outer_ticket)
            self.assertFalse(not_done)

            yield

        Proactor.get_proactor().run(test())

    def test_bottom_up_compaction(self):
        def test():

            persister = Persister()
            persister.add_heap_hash_ring(1<<15, 10)
            persister.add_heap_hash_ring(1<<15, 10)

            root = persister.layer(0)
            leaf = persister.layer(1)

            upkeeps_observed = []
            def upkeep(rstate):
                upkeeps_observed.append((rstate.get_key(),
                    rstate.get_local_record().blob_value[0]))

            persister.set_upkeep_callback(upkeep)

            def prune(rstate):
                return False

            persister.set_prune_callback(prune)

            # place two keys, having single & multiple packets
            record = PersistedRecord()
            record.add_blob_value('')

            def not_called():
                self.assertFalse(True)

            record.blob_value[0] = 'foo value'
            self.assertTrue(
                (yield persister.put(not_called, 'foo', record)))

            record.blob_value[0] = '=' * (1 << 14)
            self.assertTrue(
                (yield persister.put(not_called, 'bar', record)))

            # precondition: leaf ring is empty
            self.assertEquals(leaf.begin_offset(), leaf.end_offset())

            # begin an iteration; the iterator will skip the empty leaf
            #  layer, and begin with 'foo' in the root layer
            ticket = persister.iteration_begin()

            # after first compaction, leaf has content
            yield persister.bottom_up_compaction()
            self.assertNotEquals(leaf.begin_offset(), leaf.end_offset())
            self.assertEquals(upkeeps_observed, [])

            # validate that iterator properly followed 'foo'
            #  to the leaf layer during inner-compaction
            def iter_foo(element):
                self.assertEquals(element.key(), 'foo')
                element.parse_persisted_record(record)

            yield persister.iteration_next(
                iter_foo, ticket)
            self.assertEquals(record.blob_value[0], 'foo value')

            # after second compaction, root ring is empty
            yield persister.bottom_up_compaction()
            self.assertEquals(root.begin_offset(), root.end_offset())
            self.assertEquals(upkeeps_observed.pop(), ('foo', 'foo value'))

            # write a longer foo value into root; marks foo as dead in leaf
            record.blob_value[0] = 'a longer foo value'
            self.assertTrue(
                (yield persister.put(self._replace_callback, 'foo', record)))

            # precondition: root is no longer empty
            self.assertNotEquals(root.begin_offset(), root.end_offset())

            # validate that iterator properly followed 'bar'
            #  to the leaf layer during the second compaction
            def iter_bar(element):
                self.assertEquals(element.key(), 'bar')
                element.parse_persisted_record(record)

            yield persister.iteration_next(
                iter_bar, ticket)
            self.assertEquals(record.blob_value[0], '=' * (1 << 14))

            # we reclaim foo's dead record => no upkeep was observed;
            #  root layer is again empty
            yield persister.bottom_up_compaction()
            self.assertEquals(root.begin_offset(), root.end_offset())
            self.assertEquals(upkeeps_observed, [])

            # bar is rotated to ring tail
            yield persister.bottom_up_compaction()
            # foo is rotated to ring tail
            yield persister.bottom_up_compaction()

            # our iterator will see both keys again
            self.assertTrue((
                yield persister.iteration_next(iter_bar, ticket)))
            self.assertTrue((
                yield persister.iteration_next(iter_foo, ticket)))

            # end of iteration
            self.assertFalse((
                yield persister.iteration_next(not_called, ticket)))

            prunes_observed = []

            # switch to a prune method which discards the record
            def prune(record):
                prunes_observed.append(record.blob_value[0])
                return True

            persister.set_prune_callback(prune)

            yield persister.bottom_up_compaction()
            self.assertEquals(prunes_observed.pop(), '=' * (1 << 14))

            yield persister.bottom_up_compaction()
            self.assertEquals(prunes_observed.pop(), 'a longer foo value')

            # persister is now empty
            yield persister.bottom_up_compaction()
            self.assertEquals(prunes_observed, [])

            self.assertEquals(root.begin_offset(), root.end_offset())
            self.assertEquals(leaf.begin_offset(), leaf.end_offset())
            yield

        Proactor.get_proactor().run(test())

    def test_compaction_under_load(self):
        # excercises compaction boundary conditions, such as writing
        #  compacted elements on top of their previous location
        def test():

            seed = random.randint(0, 1<<32)
            print "Using seed ", seed
            fixture = ClusterStateFixture(seed)
            rnd = fixture.rnd

            persister = Persister()
            persister.add_heap_hash_ring(1<<12, 10)
            persister.add_heap_hash_ring(1<<12, 10)

            def upkeep(rstate): pass
            persister.set_upkeep_callback(upkeep)

            for i in xrange(500):
                record = PersistedRecord()
                record.add_blob_value(fixture.generate_bytes())
                try:
                    yield persister.put(None, fixture.generate_bytes(), record)
                except RuntimeError, e:
                    continue

            for i in xrange(500):
                yield persister.bottom_up_compaction()

            yield

        Proactor.get_proactor().run(test())

    def test_synthetic_load(self):

        seed = random.randint(0, 1<<32)
        print "Using seed ", seed

        fixture = ClusterStateFixture(seed)
        rnd = fixture.rnd

        # 300 * (4-30 byte key + expovariate(1/2048)) ~= 650,000 bytes
        keys = list(fixture.generate_name() for i in xrange(300))
        persister_values = {}

        persister = Persister()
        persister.add_heap_hash_ring(1<<15, 10)
        persister.add_heap_hash_ring(1<<16, 50)
        persister.add_heap_hash_ring(1<<20, 500)

        def validate_keyset():

            # iterate through persister, asserting we
            #  see each expected value exactly once

            expected = persister_values.copy()

            def iterate(element):

                record = PersistedRecord()
                element.parse_persisted_record(record)

                self.assertEquals(record.blob_value[0],
                    expected[element.key()])
                del expected[element.key()]

            ticket = persister.iteration_begin()
            while (yield persister.iteration_next(iterate, ticket)):
                pass

            self.assertEquals(expected, {})
            yield

        def test():

            ticket = None

            def iterate(element):

                # validate the stored record matches the
                #  current expectation for this key
                record = PersistedRecord()
                element.parse_persisted_record(record)

                self.assertEquals(record.blob_value[0],
                    persister_values.get(element.key()))

            for i in xrange(50 * len(keys)):

                # periodically validate the entire keyset via iteration
                if i % len(keys) == 0:
                    yield validate_keyset()

                # sample keyspace on exponential distribution
                ind = min(int(rnd.expovariate(
                    3.0 / len(keys))), len(keys) - 1)

                key = keys[ind]
                value = persister_values.get(key)

                op = rnd.choice(('put', 'get',
                    'drop_commit', 'drop_abort', 'iter'))

                #print 'iteration', i, key, op

                def merge(local_record, remote_record):
                    self.assertEquals(local_record.blob_value[0], value)
                    local_record.CopyFrom(remote_record)

                    return MergeResult(
                        local_was_updated = True,
                        remote_is_stale = False)

                def drop_commit(local_record):
                    self.assertEquals(local_record.blob_value[0], value)
                    return True

                def drop_abort(local_record):
                    self.assertEquals(local_record.blob_value[0], value)
                    return False

                if op == 'put':
                    record = PersistedRecord()
                    record.add_blob_value('=' * int(rnd.expovariate(1.0 / 2048)))

                    yield persister.put(merge, key, record)
                    persister_values[key] = record.blob_value[0]

                elif op == 'get':
                    record = yield persister.get(key)

                    if value is None:
                        self.assertEquals(record, None)
                    else:
                        self.assertEquals(record.blob_value[0], value)

                elif op == 'drop_abort':
                    self.assertFalse(
                        (yield persister.drop(drop_abort, key)))

                elif op == 'drop_commit':
                    was_dropped = yield persister.drop(drop_commit, key)

                    if value is None:
                        self.assertFalse(was_dropped)
                    else:
                        self.assertTrue(was_dropped)
                        del persister_values[key]

                elif op == 'iter':
                    if ticket is None:
                    	ticket = persister.iteration_begin()

                    if not (yield persister.iteration_next(iterate, ticket)):
                    	ticket = None
            yield

        Proactor.get_proactor().run(test())

    def _replace_callback(self, local_record, remote_record):
        # A merge function which always replaced local with remote
        local_record.CopyFrom(remote_record)
        return MergeResult(
            local_was_updated = True, remote_is_stale = False)

