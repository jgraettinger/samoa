
import samoa.server

import logging
log = logging.getLogger('command')

class Command(samoa.server.CommandHandler):

    @classmethod
    def check_for_error(cls, server):

        line = yield server.read_line()

        if line != 'OK\r\n':
            exc_cls_len = int((yield server.read_line()))
            exc_cls = (yield server.read_data(exc_cls_len + 2))[:-2]
            exc_msg_len = int((yield server.read_line()))
            exc_msg = (yield server.read_data(exc_msg_len + 2))[:-2]

            #raise getattr(globals(),
            #    exc_cls, RuntimeError)(exc_msg)
            raise RuntimeError("Remote Exception: " + exc_msg)

    @classmethod
    def write_error(cls, client, error):
        exc_name = error.__class__.__name__.encode('utf8')
        exc_msg = str(error).encode('utf8')

        client.queue_write('ERR\r\n%d\r\n%s\r\n%d\r\n%s\r\n' % (
            len(exc_name), exc_name, len(exc_msg), exc_msg))
        yield client.write_queued()

    @classmethod
    def handle(cls, client):
        try:
            yield cls._handle(client)
        except Exception, err:
            log.exception('<caught by Command.handle()>')
            yield cls.write_error(client, err)

        client.start_next_request()

