import uuid
import samoa
import threading

reactor = samoa.reactor();

server = samoa.server(
    "127.0.0.1",
    "5646",
    5,
    reactor,
)

server.partition = samoa.partition(
    str(uuid.uuid4()), 'test_mapped_file', 1L << 25, 1L << 17)

thread = threading.Thread(target = reactor.run)
thread.start()


try:
    while True:
        thread.join(3)
except KeyboardInterrupt:
    print "\n<Caught SIGINT; shutting down>"
    server.shutdown()
    thread.join()

