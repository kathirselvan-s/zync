"""LAN File Transfer - desktop app backend.

Run:        python app.py
Dashboard:  http://127.0.0.1:5000
"""
import os
import socket
import threading
import time

import sys
import webbrowser

from flask import Flask, jsonify, render_template, request

import config
from core.identity import Identity
from core.discovery import Discovery
from core.transfer import TransferEngine, FileSender
from core.netutil import send_msg, recv_msg
from core import state

if getattr(sys, "frozen", False):
    BASE_DIR = os.path.dirname(sys.executable)
    TEMPLATE_DIR = os.path.join(sys._MEIPASS, "templates")
    app = Flask(__name__, template_folder=TEMPLATE_DIR)
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    app = Flask(__name__)

RECEIVE_DIR_PATH = os.path.join(BASE_DIR, config.RECEIVE_DIR)
os.makedirs(RECEIVE_DIR_PATH, exist_ok=True)

identity = Identity()
identity.control_port = config.TCP_PORT

_stop = threading.Event()
discovery = Discovery(identity, config.DISCOVERY_PORT, _stop)
engine = TransferEngine(identity, RECEIVE_DIR_PATH)

_outgoing = {"sock": None, "lock": threading.Lock()}   # sender-side control conn
_current_sender = {"thread": None}


# ------------------------------------------------------------- routes

@app.route("/")
def index():
    identity.refresh_ip()
    return render_template("index.html")


@app.route("/api/identity")
def api_identity():
    identity.refresh_ip()
    return jsonify({"ip": identity.ip, "name": identity.name,
                    "device_id": identity.device_id, "tcp_port": config.TCP_PORT,
                    "udp_port": config.UDP_DATA_PORT})


@app.route("/api/identity", methods=["POST"])
def api_set_name():
    identity.set_name(request.json.get("name", ""))
    return jsonify({"ok": True, "name": identity.name})


@app.route("/api/status")
def api_status():
    snap = state.snapshot()
    snap["identity"] = {"ip": identity.refresh_ip(), "name": identity.name}
    return jsonify(snap)


@app.route("/api/connect", methods=["POST"])
def api_connect():
    target = (request.json.get("ip") or "").strip()
    if not target:
        return jsonify({"ok": False, "reason": "no IP given"}), 400
    try:
        socket.inet_aton(target)
    except OSError:
        return jsonify({"ok": False, "reason": "invalid IP"}), 400

    # --- setup-time: verify the UDP data path first (file travels over UDP) ---
    udp_ack = engine.udp_probe(target)
    udp_ok = udp_ack is not None
    if udp_ok:
        state.log("UDP probe OK: %s (%s)" % (udp_ack.get("name", "?"), target))
    else:
        state.log("WARNING: UDP probe to %s failed - file data may not arrive" % target)

    with _outgoing["lock"]:
        if _outgoing["sock"] is not None:
            try:
                _outgoing["sock"].close()
            except OSError:
                pass
            _outgoing["sock"] = None

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        try:
            sock.connect((target, config.TCP_PORT))
        except OSError as exc:
            return jsonify({"ok": False, "reason": "unreachable: %s" % exc}), 502

        identity.refresh_ip()
        send_msg(sock, {"type": "HELLO", "name": identity.name,
                        "ip": identity.ip, "device_id": identity.device_id})
        ack = recv_msg(sock)
        sock.settimeout(None)
        if not ack or ack.get("type") != "HELLO_ACK":
            sock.close()
            return jsonify({"ok": False, "reason": "no handshake reply"}), 502
        _outgoing["sock"] = sock

    state.set_connection("connected", target, ack.get("name", "?"), "outgoing")
    with state._lock:
        state.connection["udp_ok"] = udp_ok
    state.log("Connected to %s (%s) via TCP control, UDP data %s"
              % (ack.get("name", "?"), target, "OK" if udp_ok else "BLOCKED"))
    return jsonify({"ok": True, "peer_name": ack.get("name", "?"),
                    "peer_ip": target, "udp_ok": udp_ok,
                    "protocol": {"discovery": "UDP broadcast :%d" % config.DISCOVERY_PORT,
                                 "handshake_probe": "UDP :%d" % config.UDP_DATA_PORT,
                                 "control": "TCP :%d" % config.TCP_PORT,
                                 "file_data": "UDP :%d (%d streams, %d KB chunks)"
                                              % (config.UDP_DATA_PORT,
                                                 config.STREAM_COUNT,
                                                 config.CHUNK_SIZE // 1024)}})


@app.route("/api/disconnect", methods=["POST"])
def api_disconnect():
    with _outgoing["lock"]:
        if _outgoing["sock"] is not None:
            try:
                _outgoing["sock"].close()
            except OSError:
                pass
            _outgoing["sock"] = None
    state.set_connection("idle")
    state.log("Disconnected")
    return jsonify({"ok": True})


@app.route("/api/send", methods=["POST"])
def api_send():
    path = (request.json.get("path") or "").strip().strip('"')
    target = state.connection.get("peer_ip")
    if state.connection.get("status") != "connected" or not target:
        return jsonify({"ok": False, "reason": "not connected"}), 400
    if not os.path.isfile(path):
        return jsonify({"ok": False, "reason": "file not found: %s" % path}), 404
    if state.transfer.get("active"):
        return jsonify({"ok": False, "reason": "transfer already running"}), 409
    with _outgoing["lock"]:
        sock = _outgoing["sock"]
    if sock is None:
        return jsonify({"ok": False, "reason": "not connected"}), 400

    sender = FileSender(path, target, sock)
    _current_sender["thread"] = sender
    sender.start()
    state.log("Send started: %s -> %s" % (os.path.basename(path), target))
    return jsonify({"ok": True, "file": os.path.basename(path)})


@app.route("/api/cancel", methods=["POST"])
def api_cancel():
    th = _current_sender["thread"]
    if th and th.is_alive():
        th.cancel()
        return jsonify({"ok": True})
    return jsonify({"ok": False, "reason": "no active transfer"})


@app.route("/api/open-received", methods=["POST"])
def api_open_received():
    try:
        os.makedirs(RECEIVE_DIR_PATH, exist_ok=True)
        if os.name == "nt":
            os.startfile(RECEIVE_DIR_PATH)
        else:
            import subprocess
            subprocess.Popen(["xdg-open", RECEIVE_DIR_PATH])
        return jsonify({"ok": True, "path": RECEIVE_DIR_PATH})
    except Exception as exc:
        return jsonify({"ok": False, "reason": str(exc)})


# ------------------------------------------------------------- startup

def _open_browser():
    time.sleep(1.2)
    try:
        webbrowser.open("http://127.0.0.1:5000")
    except Exception:
        pass


def main():
    identity.refresh_ip()
    engine.start()
    discovery.start()
    state.log("=" * 40)
    state.log("LAN File Transfer started")
    state.log("My IP: %s   Name: %s" % (identity.ip, identity.name))
    state.log("Dashboard: http://127.0.0.1:5000")
    state.log("Receive folder: %s" % RECEIVE_DIR_PATH)
    threading.Thread(target=_open_browser, daemon=True).start()
    try:
        app.run(host="127.0.0.1", port=5000, threaded=True)
    finally:
        _stop.set()


if __name__ == "__main__":
    main()
