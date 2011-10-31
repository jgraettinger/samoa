
import unittest
import random
import uuid

from samoa.core.protobuf import PersistedRecord
from samoa.core.proactor import Proactor
from samoa.persistence.persister import Persister
from samoa.datamodel.merge_func import MergeResult

class TestPersister(unittest.TestCase):

    def setUp(self):

        self.persister = Persister()
        self.persister.add_heap_hash(1<<14, 10)
        self.persister.add_heap_hash(1<<16, 1000)

    def test_basic(self):

        def test():

            expected = PersistedRecord()
            expected.add_blob_value('bar')

            def merge_not_called(local_record, remote_record):
                self.assertFalse(True)

            # Putting a new key succeeds w/o merge callback
            self.assertTrue(
                (yield self.persister.put(merge_not_called, 'foo', expected)))

            self.assertEquals('bar',
                (yield self.persister.get('foo')).blob_value[0])

            def merge_called(local_record, remote_record):
                self.assertEquals(local_record.blob_value[0], 'bar')
                self.assertEquals(remote_record.blob_value[0], 'baz')

                local_record.CopyFrom(remote_record)

                return MergeResult(
                    local_was_updated = True,
                    remote_is_stale = False)

            # Putting an update invokes the merge callback
            expected.blob_value[0] = 'baz'
            self.assertTrue(
                (yield self.persister.put(merge_called, 'foo', expected)))

            self.assertEquals('baz',
                (yield self.persister.get('foo')).blob_value[0])

            def drop_not_called(local_record):
                self.assertFalse(True)

            # Attempt to drop an unknown key returns None
            self.assertEquals(None,
                (yield self.persister.drop(drop_not_called, 'bar')))

            def drop_rollback(local_record):
                self.assertEquals(local_record.blob_value[0], 'baz')
                return False

            # Attempt to drop 'foo', but don't commit; returns None
            self.assertEquals(None,
                (yield self.persister.drop(drop_rollback, 'foo')))

            # Key / value are still present
            self.assertEquals('baz',
                (yield self.persister.get('foo')).blob_value[0])

            def drop_commit(local_record):
                self.assertEquals(local_record.blob_value[0], 'baz')
                return True

            # This time, record is dropped & returned
            result = yield self.persister.drop(drop_commit, 'foo')
            self.assertEquals('baz', result.blob_value[0])

            # Key / value are no longer present
            self.assertFalse((yield self.persister.get('foo')))

            yield        

        Proactor.get_proactor().run_test(test)

    def test_churn(self):

        keys = [str(uuid.uuid4()) for i in xrange(300)]
        values = {}

        def test():

            # Randomly churn, dropping & setting keys
            for i in xrange(50 * len(keys)):

                # sample key on exponential distribution
                ind = min(int(random.expovariate(
                    3.0 / len(keys))), len(keys) - 1)

                key = keys[ind]
                value = values.get(key)

                choice = random.choice(('put', 'get', 'drop'))

                def merge(local_record, remote_record):
                    self.assertEquals(local_record.blob_value[0], value)
                    local_record.CopyFrom(remote_record)

                    return MergeResult(
                        local_was_updated = True,
                        remote_is_stale = False)

                def drop(local_record):
                    return True

                if choice == 'put':

                    new_rec = PersistedRecord()
                    new_rec.add_blob_value('=' * min(350,
                        int(random.expovariate(1.0 / 135))))

                    yield self.persister.put(merge, key, new_rec)
                    values[key] = new_rec.blob_value[0]

                elif choice == 'drop':

                    dropped_rec = yield self.persister.drop(drop, key)

                    if value is None:
                        self.assertEquals(dropped_rec, None)
                    else:
                        self.assertEquals(dropped_rec.blob_value[0], value)
                        del values[key]

                elif choice == 'get':

                    rec = yield self.persister.get(key)

                    if value is None:
                        self.assertEquals(rec, None)
                    else:
                        self.assertEquals(rec.blob_value[0], value)

            # iterate through persister, asserting we see each expected value 
            ticket = self.persister.begin_iteration()

            while True:

                raw_record = yield self.persister.iterate(ticket)

                if not raw_record:
                    break

                rec = PersistedRecord()
                rec.ParseFromBytes(raw_record.value)

                self.assertEquals(rec.blob_value[0], values[raw_record.key])
                del values[raw_record.key]

            self.assertEquals(values, {})
            yield

        Proactor.get_proactor().run_test(test)

