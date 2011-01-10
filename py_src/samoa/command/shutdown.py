
import samoa.command

class Shutdown(samoa.command.Command):

    def request(self, server):
        server.queue_write('shutdown\r\n')
        yield server.write_queued()
        yield self.check_for_error(server)

    @classmethod
    def _handle(cls, client):
        client.queue_write('OK\r\n')
        yield client.write_queued()
        client.context.proactor.shutdown()

