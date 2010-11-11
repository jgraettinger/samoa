
import samao.exception

class REPL(object):

    def __init__(self, key, value, rfactor):
        
        self.key = key
        self.value = value
        self.rfactor = rfactor

    def execute(self, partition_router):

        partition = partition_router.route_key(self.key)[rfactor]

        if partition.is_local:
            partition.set(self.key, self.value)
            return 'okay'


    
        for r, partitions

