
import getty
import context
import protocol

from _server import Listener

getty.Extension(Listener).requires(
    context = context.Context,
    protocol = protocol.Protocol)

