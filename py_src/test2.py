#import pdb; pdb.set_trace()

import samoa.core
import samoa.server

import getty
import random


class Scope(object):

    def __init__(self):
        print "scope_ctor"
    def __del__(self):
        print "scope_del"

class Get(samoa.server.CommandHandler):

    def handle(self, client):
        print "Get.handle() called %r %r" % (self, client)
        s = Scope()
        res1 = yield client.read_until('\n', 100)
        print "res1: %r" % res1
        res2 = yield client.read_until('\n', 100)
        print "res2: %r" % res2
        client.start_next_request()

port = 54321
print "port: ", port

proactor = samoa.core.Proactor()
context = samoa.server.Context(proactor)
protocol = samoa.server.SimpleProtocol()

protocol.add_command_handler('get', Get())

listener = samoa.server.Listener('0.0.0.0',
    str(port), 1, context, protocol)

proactor.run()

