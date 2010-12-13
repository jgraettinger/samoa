
import samoa.command.repl
import samoa.exception

class SET(object):

    def __init__(self, key, value):

        self.key = key
        self.value = value

    def execute(self, partition_router):

        partitions = partition_router.route_key(self.key)

        for r, partition in enumerate(partitions):

            if partition.is_local:

    def serialize(self):
        return 'SET %d\r\n%s\r\n%d\r\n%s\r\n' % (
            len(self.key), self.key, len(self.value), self.value)

    @classmethod
    def read_remote_response(self, remote):
        line = remote.readline()

        if not line:
            raise samoa.exception.RemoteClose()

        if line.startswith('error'):
            toks = line.split(' ', 3)
            raise getattr(samoa.exception,
                toks[1], RuntimeError)(toks[2])

        return line.strip()

