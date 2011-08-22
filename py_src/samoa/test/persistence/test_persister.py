
import unittest
import random
import uuid

from samoa.core.protobuf import PersistedRecord
from samoa.core.proactor import Proactor
from samoa.persistence.persister import Persister

class TestPersister(unittest.TestCase):

    def setUp(self):

        self.persister = Persister()
        self.persister.add_heap_hash(1<<14, 10)
        self.persister.add_heap_hash(1<<16, 1000)

    def test_basic(self):

        def test():

            expected = PersistedRecord()
            expected.add_blob_value('bar')

            def merge1(cur_rec, new_rec):
                # should not be called
                self.assertFalse(True)

            self.assertTrue(
                (yield self.persister.put(merge1, 'foo', expected)))

            self.assertEquals('bar',
                (yield self.persister.get('foo')).blob_value[0])

            def merge2(cur_rec, new_rec):
                self.assertEquals(cur_rec.blob_value[0], 'bar')
                self.assertEquals(new_rec.blob_value[0], 'baz')
                return new_rec

            expected.blob_value[0] = 'baz'
            self.assertTrue(
                (yield self.persister.put(merge2, 'foo', expected)))

            self.assertEquals('baz',
                (yield self.persister.get('foo')).blob_value[0])

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

                def merge(cur_rec, new_rec):
                    self.assertEquals(cur_rec.blob_value[0], value)
                    return new_rec

                if choice == 'put':

                    new_rec = PersistedRecord()
                    new_rec.add_blob_value('=' * min(350,
                        int(random.expovariate(1.0 / 135))))

                    yield self.persister.put(merge, key, new_rec)
                    values[key] = new_rec.blob_value[0]

                elif choice == 'drop':

                    dropped_rec = yield self.persister.drop(key)

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

                records = yield self.persister.iterate(ticket)
                if not records:
                    break

                for raw_rec in records:
                    rec = PersistedRecord()
                    rec.ParseFromBytes(raw_rec.value)

                    self.assertEquals(rec.blob_value[0], values[raw_rec.key])
                    del values[raw_rec.key]

            self.assertEquals(values, {})
            yield

        Proactor.get_proactor().run_test(test)

