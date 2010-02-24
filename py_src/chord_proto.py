
from twisted.internet import protocol, reactor
from twisted.protocols.basic import LineReceiver
import bisect
import random
random.seed(25)

host = 'localhost'
port_start = 15000

server_count    = 2
partition_count = 5

start_conns = [i for i in xrange(server_count)]
random.shuffle( start_conns)


class ChordProtocol(LineReceiver):
    
    def connectionMade(self):
        self.partitions = []
        self.send('ident', self.factory.host, self.factory.port, self.factory.datacenter)
    
    def connectionLost(self, reason):
        self.factory.on_disconnect(self)
    
    def lineReceived(self, line):
        parts = line.strip().split()
        
        if parts[0] == 'ident':
            self.peer_host = parts[1]
            self.peer_port = int(parts[2])
            self.peer_dc   = parts[3]
            self.factory.on_peer_connect(self, self.peer_host, self.peer_port, self.peer_dc)
            return
        
        getattr(self.factory, 'on_%s' % parts[0], self, parts[1:])
    
    def send(self, type, *args):
        self.transport.write(
            '%s%s%s\r\n' % (type, '\t' if args else '',
            '\t'.join(str(i) for i in args)))

class Peer(object):
    __slots__ = ['host', 'port', 'ref_cnt', 'proto']
    def __init__(self, host, port, dc):
        self.host = host
        self.port = port
        self.ref_cnt = 0
        self.proto = None
        self.datacenter = dc


class Router(object):
    __slots__ = ['ring', 'ring_size']
    
    def __init__(self, ring_size):
        self._ring = []
        self._rsize = ring_size
    
    def route_key(self, key):
        return self._ring[ bisect.bisect_left(
            self._ring, (hash(key),)) % len(ring)]
    
    def has_marker(self, pos_begin, pos_end):
        # Note pos_begin may be > pos_end
        return False
    
    def add_local_locations(self, new_locations):
        
        migrations = {}
        for (n_pos, n_addr) in new_locations:
            
            ind = bisect.bisect_left( ring, (n_pos, n_addr)):
            if ind != len(ring) and ring[ind] == (n_pos, n_addr, False):
                # we already knew about this location
                continue
            
            ring.insert(ind, (n_pos, n_addr, False))
            succ_ind = (ind + 1) % len(ring)
            
            # Is location's successor a local partition?
            if ring[succ_ind][2]:
                # local partition's keyspace has shrunk
                migrations[ring[succ_ind][0]] = n_pos
        
        return migrations
    
    def add_remote_locations(self, new_locations):
        
        host_ref_delta = {}
        migrations = {}
        
        # self.ring:
        #  (pos, (host, port), is_local)
        
        # loc is (pos, (host, port))
        for (n_pos, n_addr) in new_locations:
            
            # Insertion update - we insert location iff
            #  * prior is a local partition
            #  * successor is a local partition
            #  * exists a marker b/w location & prior
            
            ind = bisect.bisect_left( ring, (n_pos, n_addr))
            if ind != len(ring) and ring[ind] == (n_pos, n_addr, False):
                # we already knew about this location
                continue
            
            prior_ind = (ind + len(ring) - 1) % len(ring)
            succ_ind  = ind % len(ring)
            
            # location which *may* not be
            #  needed due to new location
            rem_ind = None
            
            # Is location's successor a local partition?
            if ring[succ_ind][2]:
                ring.insert(ind, (n_pos, n_addr, False))
                succ_ind = (ind + 1) % len(ring)
                
                # local partition's keyspace has shrunk
                migrations[ring[succ_ind][0]] = n_pos
                
                # loc's prior is a removal candidate
                if ring[prior_ind]
                rem_ind = prior_ind
            
            # Is location's prior a local partition?
            elif ring[prior_ind][2]:
                ring.insert(ind, (n_pos, n_addr, False))
                succ_ind = (ind + 1) % len(ring)
                
                # loc's successor is a removal candidate
                rem_ind = succ_ind
            
            # Is there a marker between location & prior?
            elif self.has_marker(ring[prior_ind][0], n_pos):
                ring.insert(ind, (n_pos, n_addr, False))
                succ_ind = (ind + 1) % len(ring)
                
                # loc's successor is a removal candidate
                rem_ind = succ_ind
            
            if rem_ind == None:
                # no insertion occurred => we're done
                continue
            
            host_ref_delta.setdefault(n_addr, 0)
            host_ref_delta[n_addr] += 1
            
            # Removal update - we keep rem_ind iff
            #  * prior is a local partition
            #  * successor is a local partition
            #  * exists a marker b/w rem_ind and prior
            
            ind = rem_ind
            prior_ind = (ind + len(ring) - 1) % len(ring)
            succ_ind  = (ind + 1) % len(ring)
            
            if ring[prior_ind][2] or ring[succ_ind][2]:
                continue
            if self.has_marker(ring[prior_ind][0], ring[ind][0]):
                continue
            
            host_ref_delta.setdefault(ring[ind][1], 0)
            host_ref_delta[ring[ind][1]] -= 1
            
            ring.pop(ind)
        
        host_ref_delta = dict(i for i in host_ref_delta.iteritems() if i[1])
        return host_ref_delta, migrations
    
    def remove_remote_locations():
        return

class ChordServer(protocol.ClientFactory):
    __slots__ = ['partitions', 'peers', 'host', 'port']
    
    protocol = ChordProtocol
    
    def __init__(self, host, port):
        
        self.partitions = sorted([random.randint(0, 10000) for i in xrange(partition_count)])
        
        self.peers = {}
        self.host = host
        self.port = port
        self.datacenter = 'A'
        
        reactor.listenTCP( port, self, backlog = server_count)
        return
    
    def log(self, msg, *args):
        print ("%%s:%%s - %s" % msg) % (
            (self.host, self.port) + args)
    
    def new_peer(self, peer_host, peer_port, peer_dc):
        
        peer_addr = (peer_host, peer_port)
        assert peer_addr not in self.peers
        
        self.peers[peer_addr] = Peer(peer_host, peer_port, peer_dc)
        reactor.connectTCP(peer_host, peer_port, self)
    
    def on_peer_connect(self, proto, peer_host, peer_port, peer_dc):
        self.log('connected to %s:%s', peer_host, peer_port)
        
        addr = (peer_host, peer_port)
        if addr not in self.peers:
            self.peers[addr] = Peer(peer_host, peer_port, peer_dc)
        
        self.peers[addr].proto = proto
        proto.send('notify_request')
    
    def on_disconnect(self, proto):
        self.log('disconnected from %s:%s', proto.p_host, proto.p_port)
    
    def on_notify_request(self, proto):
        proto.send('notify', self.partitions)
    
    def on_notify(self, proto, ring):
        ring = eval(ring)
        
        for key_ind in ring:

            
            
            
        
        

servers = [ChordServer(host, port_start + i) for i in xrange(server_count)]

for server, seed in zip(servers, start_conns):
    server.new_peer(host, port_start + seed)

reactor.run()
