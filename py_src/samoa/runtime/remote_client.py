
import logging
import gevent.event
import gevent.socket as socket
import samoa.command

class RemoteClient(object):

    log = logging.getLogger('remote_client')

    __slots__ = ['server', 'sock', 'host', 'port']

    def __init__(self, server, sock):

        self.server = server
        self.sock = sock
        self.host, self.port = sock.getpeername()

        self.sock.send('SAMOA\r\n')

        gevent.spawn(self._service_loop)
        return

    def close(self):

        if self.sock:
            self.sock.close()
        self.sock = None
        return

    def _service_loop(self):

        sfile = self.sock.makefile()

        while True:
            cmd = sfile.readline()
            cmd = getattr(samoa.command, cmd.strip(), None)

            if not cmd:
                sfile.write("-ERR no such command\r\n")
                self.close()
                return

            try:
                cmd = cmd.request_from_stream(sfile)
                resp = cmd.execute(self.server)
                cmd.response_to_stream(self.sock.send, resp)

            except Exception, e:
                self.log.exception('While servicing request: %r' % cmd)
                cmd.error_to_stream(self.sock.send, e)

