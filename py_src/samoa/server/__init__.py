
import getty

import samoa.core
from _server import *

getty.Extension(Context).requires(
    proactor = samoa.core.Proactor)

getty.Extension(Listener).requires(
    host = getty.Config('host'),
    port = getty.Config('port'),
    listen_backlog = getty.Config('listen_backlog'),
    context = Context,
    protocol = Protocol)

