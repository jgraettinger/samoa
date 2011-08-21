
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

        keys = [str(uuid.uuid4()) for i in xrange(600)]
        values = {}

        def test():

            # Randomly churn, dropping & setting keys
            for i in xrange(10 * len(keys)):

                # sample key on exponential distribution
                ind = min(int(random.expovariate(
                    2.0 / len(keys))), len(keys) - 1)

                key = keys[ind]
                value = values.get(key)

                choice = random.randint(0, 2)

                if choice == 0:

                    new_val = '=' * min(350,
                        int(random.expovariate(1.0 / 135)))

                    def on_put(cur_rec, new_rec):

                        self.assertTrue((not cur_rec and not value) \
                            or (cur_rec and cur_rec.value == value))

                        new_rec.set_value(new_val)
                        values[new_rec.key] = new_val
                        return 1

                    try:
                        yield self.persister.put(on_put, key, len(new_val))
                    except Exception, e:
                        print e

                elif choice == 1:

                    def on_drop(cur_rec):

                        self.assertTrue((not cur_rec and not value) \
                            or (cur_rec and cur_rec.value == value))

                        if cur_rec:
                            del values[cur_rec.key]

                        return 1

                    yield self.persister.drop(on_drop, key)

                elif choice == 2:

                    def on_get(cur_rec):

                        self.assertTrue((not cur_rec and not value) \
                            or (cur_rec and cur_rec.value == value))

                    yield self.persister.get(on_get, key)

            # iterate through persister, asserting we see each expected value 
            def on_iterate(records):

                for rec in records:
                    self.assertEquals(rec.value, values[rec.key])
                    del values[rec.key]

            ticket = self.persister.begin_iteration()
            while (yield self.persister.iterate(on_iterate, ticket)):
                pass

            self.assertEquals(values, {})
            yield

        Proactor.get_proactor().run_test(test)

