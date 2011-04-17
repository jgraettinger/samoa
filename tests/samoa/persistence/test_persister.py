
import unittest
import random
import uuid

from samoa.core import Proactor
from samoa.persistence.persister import Persister

class TestPersister(unittest.TestCase):

    def setUp(self):
        self.proactor = Proactor()
        self.persister = Persister(self.proactor)
        self.persister.add_heap_hash(1<<15, 100)
        self.persister.add_heap_hash(1<<16, 1000)

    def test_basic(self):

        def test():
            yield self.persister.put(
                lambda cr, nr: nr.set_value('bar') or 1, 'foo', 3)
            yield self.persister.get(
                lambda r: self.assertEquals(r.value, 'bar'), 'foo')
            self.proactor.shutdown()
            yield

        self.proactor.spawn(test)
        self.proactor.run()

    def test_churn(self):

        data = {}

        for i in xrange(1000):
            data[str(uuid.uuid4())] = '=' * int(
                random.expovariate(1.0 / 135))

        data_size = sum(len(i) + len(j) for i,j in data.iteritems())

        def test():

            # Set all keys / values
            for key, value in data.items():
                try:
                    yield self.persister.put(lambda cr, nr: \
                        (nr.set_value(data[nr.key]) or 1),
                        key, len(value))
                except:
                    import pdb; pdb.set_trace()
    
            # Randomly churn, dropping & setting keys
            for i in xrange(5 * len(data)):
                drop_key, set_key, get_key = random.sample(data.keys(), 3)
    
                yield self.persister.drop(lambda r: 1, drop_key)
                
                yield self.persister.put(lambda cr, nr: \
                    (nr.set_value(data[nr.key]) or 1),
                    put_key, int(len(data[put_key]) * 1.1))
    
                yield self.persister.get(lambda r: \
                    (not r or self.assertEquals(r.value, data[r.key])),
                    get_key)
    
            # Set all keys / values
            for key, value in data.items():
                yield self.persister.put(lambda cr, nr: \
                    (nr.set_value(value) or 1), key,
                    int(len(value) * 1.1))
    
            # contents of rolling hash should
            #   be identical to data
            #self.assertEquals(data, self._dict(h))

            self.proactor.shutdown()

        self.proactor.spawn(test)
        self.proactor.run()

