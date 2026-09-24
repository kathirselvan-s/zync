"""Small networking helpers shared by all modules."""
import json
import struct
import socket


def get_local_ip():
    """Best-effort detection of the local LAN IPv4 address."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))          # no traffic actually sent
        ip = s.getsockname()[0]
    except OSError:
        ip = "127.0.0.1"
    finally:
        s.close()
    return ip


def recv_exact(sock, n):
    """Receive exactly n bytes or return None on close/error."""
    buf = bytearray()
    while len(buf) < n:
        try:
            chunk = sock.recv(n - len(buf))
        except (ConnectionResetError, BrokenPipeError, OSError):
            return None
        if not chunk:
            return None
        buf.extend(chunk)
    return bytes(buf)


def send_msg(sock, obj):
    """Send one JSON message, length-prefixed."""
    data = json.dumps(obj).encode("utf-8")
    sock.sendall(struct.pack("!I", len(data)) + data)


def recv_msg(sock):
    """Receive one length-prefixed JSON message, or None if peer closed."""
    hdr = recv_exact(sock, 4)
    if hdr is None:
        return None
    (n,) = struct.unpack("!I", hdr)
    data = recv_exact(sock, n)
    if data is None:
        return None
    try:
        return json.loads(data.decode("utf-8"))
    except ValueError:
        return None
