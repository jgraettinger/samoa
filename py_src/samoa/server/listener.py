
import getty
from _server import Listener

import context
import protocol

getty.Extension(Listener).requires(
    host = getty.Config('host'),
    port = getty.Config('port'),
    listen_backlog = getty.Config('listen_backlog'),
    context = context.Context,
    protocol = protocol.Protocol)

