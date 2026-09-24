"""UDP broadcast peer discovery.

Every node broadcasts a small beacon (name + ip + ports) on the LAN;
every node listens and maintains the shared peers table with TTL expiry.
"""
import json
import socket
import threading
import time

from . import state

BEACON_INTERVAL = 2.0
PEER_TTL = 6.0
BROADCAST_ADDR = "255.255.255.255"


class Discovery(threading.Thread):
    def __init__(self, identity, port, stop_event):
        super().__init__(daemon=True)
        self.identity = identity
        self.port = port
        self.stop_event = stop_event

    def run(self):
        beacon_sock = self._make_beacon_socket()
        listen_sock = self._make_listen_socket()
        if listen_sock is None:
            state.log("Discovery listener failed to start")
            return
        last_beacon = 0.0
        listen_sock.settimeout(0.5)
        state.log("Discovery active on UDP port %d" % self.port)
        while not self.stop_event.is_set():
            now = time.time()
            # --- broadcast my beacon ---
            if beacon_sock is not None and now - last_beacon >= BEACON_INTERVAL:
                last_beacon = now
                try:
                    self.identity.refresh_ip()
                    beacon_sock.sendto(
                        json.dumps(self.identity.beacon_payload()).encode("utf-8"),
                        (BROADCAST_ADDR, self.port))
                except OSError:
                    pass
            # --- listen for peer beacons ---
            try:
                data, addr = listen_sock.recvfrom(4096)
            except socket.timeout:
                data = None
            except OSError:
                break
            if data:
                self._handle_beacon(data, addr[0])
            self._expire(now)
        for s in (beacon_sock, listen_sock):
            try:
                s.close()
            except OSError:
                pass

    def _handle_beacon(self, data, src_ip):
        try:
            msg = json.loads(data.decode("utf-8"))
            if msg.get("type") != "beacon":
                return
            if msg.get("device_id") == self.identity.device_id:
                return
        except (ValueError, UnicodeDecodeError):
            return
        with state._lock:
            state.peers[src_ip] = {
                "name": str(msg.get("name", "unknown"))[:40],
                "device_id": msg.get("device_id"),
                "last_seen": time.time(),
                "control_port": msg.get("control_port"),
            }

    def _expire(self, now):
        with state._lock:
            stale = [ip for ip, p in state.peers.items()
                     if now - p["last_seen"] > PEER_TTL]
            for ip in stale:
                del state.peers[ip]
                state.log("Peer gone: %s" % ip)

    def _make_beacon_socket(self):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            return s
        except OSError:
            return None

    def _make_listen_socket(self):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(("0.0.0.0", self.port))
            return s
        except OSError:
            return None
