
import samoa.exception

class Command(object):

    @classmethod
    def check_error(cls, sin, line):

        if not line:
            raise samoa.exception.RemoteClose()

        if line.startswith('ERR'):
            toks = line.split(' ', 3)
            raise getattr(samoa.exception,
                toks[1], RuntimeError)(toks[2])

    @classmethod
    def error_to_stream(cls, sout, error):
        exc_name = error.__class__.__name__.encode('utf8')
        exc_msg = error.message.encode('utf8')

        sout('ERR\r\n%d\r\n%s\r\n%d\r\n%s\r\n' % (
            len(exc_name), exc_name, len(exc_msg), exc_msg))

