
import samoa.command

class Echo(samoa.command.Command):

    def __init__(self):
        samoa.command.Command.__init__(self)
        self.data = ''

    def request(self, server):

        server.queue_write('echo\r\n%d\r\n%s\r\n' % (
            len(self.data), self.data))
        yield server.write_queued()

        yield self.check_for_error(server)

        l = int((yield server.read_line()))
        yield (yield server.read_data(l + 2))[:-2]

    @classmethod
    def _handle(cls, client):
        l = int((yield client.read_line()))
        data = (yield client.read_data(l + 2))[:-2]
        client.queue_write('OK\r\n%d\r\n%s\r\n' % (l, data))
        yield client.write_queued()

