
import samoa.exception

import command

class GET(command.Command):

    def __init__(self, tbl, key):
        self.tbl = tbl
        self.key = key

    @classmethod
    def request_from_stream(cls, sin):
        tbl_len = int(sin.readline())
        tbl = sin.read(tbl_len)
        sin.read(2)
        key_len = int(sin.readline())
        key = sin.read(key_len)
        sin.read(2)
        return GET(tbl, key)

    def request_to_stream(self, sout):
        sout('GET\r\n%d\r\n%s\r\n%d\r\n%s\r\n' % (
            len(self.tbl), self.tbl, len(self.key), self.key))

    @classmethod
    def response_from_stream(cls, sin):
        line = sin.readline()
        cls.check_for_error(sin, line)

        val_len = int(line)
        if val_len == -1:
            return None

        val = sin.read(val_len)
        sin.read(2)
        return val

    @classmethod
    def response_to_stream(cls, sout, response):
        if respose is None:
            sout('-1\r\n')
        else:
            sout('%d\r\n%s\r\n' % (len(response), response))

    def execute(self, server):

        table = server.tables.get(self.tbl)
        partitions = list(table.route_key(self.key))

        # attempt to route locally
        for partition in partitions:
            if partition.is_local and partition.is_online:
                return partition.get(self.key)

        # route remotely
        for partition in partitions:
            if partition.is_online:
                return partition.forward(self).get()

        raise samoa.exception.Unroutable(self.key)

