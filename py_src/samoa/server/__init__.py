
import getty
from context import Context

from _server import Listener, Protocol, CommandHandler

getty.Extension(Listener).requires(
    host = getty.Config('host'),
    port = getty.Config('port'),
    listen_backlog = getty.Config('listen_backlog'),
    context = Context,
    protocol = Protocol)

