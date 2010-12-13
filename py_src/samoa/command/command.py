
import samoa.exception

class Command(object):

    @classmethod
    def check_for_error(cls, sin, line):

        if not line:
            raise samoa.exception.RemoteClose()

        if line == 'ERR\r\n':
            exc_cls_len = sin.readline().strip()
            exc_cls = sin.read(int(exc_cls_len))
            sin.read(2)
            exc_msg_len = sin.readline().strip()
            exc_msg = sin.read(int(exc_msg_len))
            sin.read(2)

            raise getattr(samoa.exception,
                exc_cls, RuntimeError)(exc_msg)

    @classmethod
    def error_to_stream(cls, sout, error):
        exc_name = error.__class__.__name__.encode('utf8')
        exc_msg = str(error).encode('utf8')

        sout('ERR\r\n%d\r\n%s\r\n%d\r\n%s\r\n' % (
            len(exc_name), exc_name, len(exc_msg), exc_msg))

