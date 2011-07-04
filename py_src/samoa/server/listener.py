
import getty
import context
import protocol

import _server

class Listener(_server.Listener):

    @getty.requires(
        context = context.Context,
        protocol = protocol.Protocol)
    def __init__(self, context, protocol):
        _server.Listener.__init__(self, context, protocol)
        context.get_tasklet_group().start_managed_tasklet(self)

