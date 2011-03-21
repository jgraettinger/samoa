
import getty
import samoa.core
from _client import ServerPool

getty.Extension(ServerPool).requires(
    proactor = samoa.core.Proactor)

