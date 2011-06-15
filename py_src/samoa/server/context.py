
import getty
import samoa.core.protobuf
from _server import Context

getty.Extension(Context).requires(
    cluster_state = samoa.core.protobuf.ClusterState)

