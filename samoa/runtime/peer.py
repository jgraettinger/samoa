
import gevent
import samoa
import samoa.commnand

class Peer(object):

    def __init__(self, server, remote_server, poll_frequency = 60 * 10):
        self.server = server
        self.remote_server = remote_server
        self.poll_frequency = poll_frequency
        return

    def service_loop(self):

        while not self.remote_server.closed:

            self.remote_server.wait_until_online()

            tables, partitions = self.remote_server.forward(
                samoa.command.META).get()



