
import logging
import gevent.event
import gevent.queue
import gevent.socket as socket

class RemoteServer(object):

    log = logging.getLogger('remote_server')

    __slots__ = ['host', 'port', 'ready',
        '_sock', '_pending', '_ready']

    def __init__(self, host, port):

        self.host = host
        self.port = port

        gevent.spawn(self._service_loop)
        self._sock = None
        self._pending = gevent.queue.Queue()
        self._ready = gevent.event.Event()
        return

    @property
    def is_online(self):
        return self._ready and self._ready.is_set()

    def wait_until_online(self, timeout = None):
        assert self._ready, "RemoteServer has been closed"
        self._ready.wait(timeout)

    def forward(self, command):

        if not self.is_online:
            raise RuntimeError("RemoteServer is not online")

        cont = gevent.event.AsyncResult()

        self._pending.put((command, cont))
        command.request_to_stream(self._sock.send)
        return cont

    def close(self):
        self._ready = None
        cont = gevent.event.AsyncResult()
        self._pending.put(('client_shutdown', cont))
        return cont

    def _service_loop(self):

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            sock.connect((self.host, self.port))

            sfile = sock.makefile()
            assert sfile.readline().startswith('SAMOA')

        except:
            self.log.exception('while connecting')
            gevent.spawn_later(15, self._service_loop)
            return

        self.log.info('connected to %s:%s' % (self.host, self.port))

        self._sock = sock
        self._ready.set()

        while True:

            # service responses from server
            command, continuation = self._pending.get()

            if command == 'client_shutdown':
                self._sock = None
                continuation.set(None)
                return

            try:
                response = command.response_from_stream(sfile)
                continuation.set(response)

            except Exception, e:
                # We didn't undertand the remote respone, or the
                #  connection was closed

                self.log.exception('while reading command response')

                self._ready = gevent.event.Event()
                self._sock = None

                continuation.set_exception(e)

                # notify pending commands of the disconnect
                while not self._pending.empty():
                    _, continuation = self._pending.get()
                    continuation.set_exception(
                        samoa.exception.RemoteClose())

                # initiate reconnection
                gevent.spawn_later(1, self._service_loop)
                return 

