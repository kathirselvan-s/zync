"""Shared runtime state (peers, connection, transfers) plus an event log."""
import threading
import time

_lock = threading.Lock()

peers = {}          # ip -> {"name", "device_id", "last_seen", "control_port"}
connection = {      # current control connection (either direction)
    "status": "idle",        # idle | connected
    "peer_ip": None,
    "peer_name": None,
    "direction": None,       # outgoing | incoming
    "udp_ok": False,         # setup-time UDP probe result
}
transfer = {        # one active transfer at a time (V1)
    "active": False,
    "direction": None,       # send | receive
    "filename": None,
    "size": 0,
    "done_bytes": 0,
    "percent": 0.0,
    "speed_mbps": 0.0,
    "streams": 0,
    "result": None,          # running | success | failed | cancelled
    "detail": "",
    "started_at": None,
}
_log = []
MAX_LOG = 200


def log(msg):
    with _lock:
        _log.append({"t": time.strftime("%H:%M:%S"), "msg": str(msg)})
        del _log[:-MAX_LOG]


def get_log():
    with _lock:
        return list(_log)


def update_transfer(**kw):
    with _lock:
        transfer.update(kw)


def reset_transfer():
    with _lock:
        transfer.update({
            "active": False, "direction": None, "filename": None,
            "size": 0, "done_bytes": 0, "percent": 0.0, "speed_mbps": 0.0,
            "streams": 0, "result": None, "detail": "",
            "started_at": None,
        })


def set_connection(status, peer_ip=None, peer_name=None, direction=None):
    with _lock:
        connection.update({
            "status": status, "peer_ip": peer_ip,
            "peer_name": peer_name, "direction": direction,
        })


def snapshot():
    """Everything the dashboard needs in one dict."""
    with _lock:
        now = time.time()
        peer_list = [
            {"ip": ip, "name": p["name"], "age": round(now - p["last_seen"], 1)}
            for ip, p in sorted(peers.items(), key=lambda kv: kv[1]["last_seen"],
                                reverse=True)
        ]
        return {
            "peers": peer_list,
            "connection": dict(connection),
            "transfer": dict(transfer),
            "log": list(_log),
        }
