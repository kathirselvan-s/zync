"""Reliable UDP bulk transfer.

Control messages (length-prefixed JSON over TCP):
    HELLO         {name, ip, device_id}
    HELLO_ACK     {name, ip}
    TRANSFER      {transfer_id, filename, size, sha256, chunk_size,
                   streams, total_chunks}
    TRANSFER_ACK  {accepted, reason}
    MISSING       {type:"missing", transfer_id, stream, packets:[...]}
    RESULT        {type:"result", transfer_id, ok, sha256, detail}

Data plane: UDP datagrams framed by protocol.build_packet / parse_packet.
Receiver writes chunks directly into a preallocated file at offset, tracks
a per-stream bitmap, reports missing packet numbers, and verifies SHA-256.
"""
import hashlib
import json
import math
import os
import socket
import threading
import time
import random

from . import protocol, state
from .netutil import send_msg, recv_msg
import config


# ---------------------------------------------------------------- utils

def sha256_file(path, chunk=1 << 20):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def unique_path(directory, filename):
    filename = os.path.basename(filename.replace("\\", "/")) or "file"
    root, ext = os.path.splitext(filename)
    candidate = os.path.join(directory, filename)
    n = 1
    while os.path.exists(candidate):
        candidate = os.path.join(directory, "%s (%d)%s" % (root, n, ext))
        n += 1
    return candidate


def split_ranges(total_chunks, streams):
    """Divide packet numbers 0..total-1 into `streams` contiguous ranges."""
    streams = max(1, min(streams, total_chunks))
    base, rem = divmod(total_chunks, streams)
    ranges, start = [], 0
    for i in range(streams):
        count = base + (1 if i < rem else 0)
        ranges.append((start, start + count))
        start += count
    return ranges


# ---------------------------------------------------------------- sender

class FileSender(threading.Thread):
    def __init__(self, file_path, target_ip, control_sock):
        super().__init__(daemon=True)
        self.file_path = file_path
        self.target_ip = target_ip
        self.sock = control_sock
        self.stop_event = threading.Event()
        self._udp_socks = []
        self._sent_once = 0          # unique packets sent at least once
        self._sent_lock = threading.Lock()
        self._done = threading.Event()

    def cancel(self):
        self.stop_event.set()

    def run(self):
        try:
            self._run()
        except Exception as exc:                      # noqa: BLE001
            state.update_transfer(result="failed", detail=str(exc))
            state.log("Send failed: %s" % exc)

    def _run(self):
        size = os.path.getsize(self.file_path)
        total_chunks = max(1, math.ceil(size / config.CHUNK_SIZE))
        digest = sha256_file(self.file_path)
        transfer_id = random.getrandbits(63)
        ranges = split_ranges(total_chunks, config.STREAM_COUNT)
        streams = len(ranges)

        state.update_transfer(
            active=True, direction="send",
            filename=os.path.basename(self.file_path), size=size,
            done_bytes=0, percent=0.0, speed_mbps=0.0,
            streams=streams, result="running", detail="",
            started_at=time.time())

        # --- negotiate over control channel ---
        send_msg(self.sock, {
            "type": "TRANSFER", "transfer_id": transfer_id,
            "filename": os.path.basename(self.file_path),
            "size": size, "sha256": digest,
            "chunk_size": config.CHUNK_SIZE,
            "streams": streams, "total_chunks": total_chunks,
        })
        self.sock.settimeout(config.TRANSFER_TIMEOUT)
        ack = None
        while True:
            m = recv_msg(self.sock)
            if m is None:
                break
            if m.get("type") == "TRANSFER_ACK":
                ack = m
                break
        if not ack or not ack.get("accepted"):
            raise RuntimeError("Receiver refused transfer: %s"
                               % (ack or {}).get("reason", "no response"))
        state.log("Receiver accepted. Sending %d chunks over %d streams..."
                  % (total_chunks, streams))

        receiver_udp = (self.target_ip, config.UDP_DATA_PORT)
        for _ in range(streams):
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self._udp_socks.append(s)

        t0 = time.time()
        threads = []
        for sid, (lo, hi) in enumerate(ranges):
            th = threading.Thread(target=self._send_stream,
                                  args=(sid, lo, hi, transfer_id, receiver_udp),
                                  daemon=True)
            th.start()
            threads.append(th)
        for th in threads:
            th.join()

        # --- retransmission loop: wait for MISSING / RESULT ---
        deadline = time.time() + config.TRANSFER_TIMEOUT
        self.sock.settimeout(1.0)
        result = None
        while time.time() < deadline and not self.stop_event.is_set():
            try:
                msg = recv_msg(self.sock)
            except socket.timeout:
                continue
            except (ConnectionResetError, BrokenPipeError, OSError):
                break
            if msg is None:
                break
            if msg.get("type") == "missing":
                self._resend(msg, transfer_id, receiver_udp)
            elif msg.get("type") == "result":
                result = msg
                break

        elapsed = max(time.time() - t0, 0.001)
        for s in self._udp_socks:
            try:
                s.close()
            except OSError:
                pass

        if self.stop_event.is_set():
            state.update_transfer(result="cancelled", detail="Cancelled by user")
            state.log("Transfer cancelled")
        elif result and result.get("ok"):
            state.update_transfer(result="success", percent=100.0,
                                  done_bytes=size,
                                  speed_mbps=round(size / elapsed / 1e6, 1),
                                  detail="SHA-256 verified by receiver")
            state.log("Transfer complete: %s (%s) in %.1fs"
                      % (os.path.basename(self.file_path), result.get("sha256", "")[:12], elapsed))
        else:
            state.update_transfer(result="failed",
                                  detail=(result or {}).get("detail", "timeout / no result"))
            state.log("Transfer failed: %s" % (result or {}).get("detail", "timeout"))

    def _send_stream(self, sid, lo, hi, transfer_id, receiver_udp):
        """Send every packet of one stream once (thread per stream)."""
        s = self._udp_socks[sid]
        with open(self.file_path, "rb") as f:
            for pno in range(lo, hi):
                if self.stop_event.is_set():
                    return
                f.seek(pno * config.CHUNK_SIZE)
                payload = f.read(config.CHUNK_SIZE)
                s.sendto(protocol.build_packet(transfer_id, sid, pno, payload),
                         receiver_udp)
                with self._sent_lock:
                    self._sent_once += 1
                    done = self._sent_once
                self._progress(done)

    def _resend(self, msg, transfer_id, receiver_udp):
        sid = msg.get("stream")
        packets = msg.get("packets") or []
        if not (0 <= sid < len(self._udp_socks)) or not packets:
            return
        s = self._udp_socks[sid]
        with open(self.file_path, "rb") as f:
            for pno in packets:
                if self.stop_event.is_set():
                    return
                if not isinstance(pno, int):
                    continue
                f.seek(pno * config.CHUNK_SIZE)
                payload = f.read(config.CHUNK_SIZE)
                if payload:
                    s.sendto(protocol.build_packet(transfer_id, sid, pno, payload),
                             receiver_udp)

    def _progress(self, done_chunks):
        tr = state.transfer
        size = tr.get("size") or 0
        done_bytes = min(size, done_chunks * config.CHUNK_SIZE)
        started = tr.get("started_at") or time.time()
        elapsed = max(time.time() - started, 0.001)
        state.update_transfer(
            done_bytes=done_bytes,
            percent=round(100.0 * done_bytes / size, 1) if size else 0.0,
            speed_mbps=round(done_bytes / elapsed / 1e6, 1))


# ---------------------------------------------------------------- receiver

class ReceiveSession:
    def __init__(self, meta, control_sock, incoming_dir):
        self.meta = meta
        self.sock = control_sock
        self.transfer_id = meta["transfer_id"]
        self.total = meta["total_chunks"]
        self.chunk_size = meta["chunk_size"]
        self.expected_sha = meta["sha256"]
        os.makedirs(incoming_dir, exist_ok=True)
        self.path = unique_path(incoming_dir, meta["filename"])
        self.fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
        os.ftruncate(self.fd, meta["size"])

        self.ranges = split_ranges(self.total, meta["streams"])
        self.bitmaps = [bytearray((hi - lo + 7) // 8) for lo, hi in self.ranges]
        self.received = 0
        self.lock = threading.Lock()
        self.done = threading.Event()
        self.ok = False
        self.detail = ""
        self.started_at = time.time()

        state.update_transfer(
            active=True, direction="receive", filename=meta["filename"],
            size=meta["size"], done_bytes=0, percent=0.0, speed_mbps=0.0,
            streams=meta["streams"], result="running", detail="",
            started_at=self.started_at)
        state.log("Incoming transfer: %s (%d chunks, %d streams)"
                  % (meta["filename"], self.total, meta["streams"]))

        self._reporter = None

    def start(self):
        """Begin missing-packet reporting (call only after TRANSFER_ACK)."""
        if self._reporter is None:
            self._reporter = threading.Thread(target=self._report_missing,
                                              daemon=True)
            self._reporter.start()

    # called from the engine's single UDP receive thread
    def handle_packet(self, sid, pno, payload):
        if self.done.is_set() or not (0 <= sid < len(self.ranges)):
            return
        lo, hi = self.ranges[sid]
        if not (lo <= pno < hi):
            return
        byte, bit = divmod(pno - lo, 8)
        with self.lock:
            if self.bitmaps[sid][byte] & (1 << bit):
                return
            os.lseek(self.fd, pno * self.chunk_size, os.SEEK_SET)
            os.write(self.fd, payload)
            self.bitmaps[sid][byte] |= (1 << bit)
            self.received += 1
            count = self.received
        self._progress(count)
        if count >= self.total:
            self._finish()

    def _progress(self, count):
        size = self.meta["size"]
        done = min(size, count * self.chunk_size)
        elapsed = max(time.time() - self.started_at, 0.001)
        state.update_transfer(
            done_bytes=done, percent=round(100.0 * done / size, 1),
            speed_mbps=round(done / elapsed / 1e6, 1))

    def _missing(self):
        out = []
        for sid, bm in enumerate(self.bitmaps):
            lo, _ = self.ranges[sid]
            packets = [lo + i
                       for i in range(len(bm) * 8)
                       if not (bm[i >> 3] & (1 << (i & 7)))
                       and i < self.ranges[sid][1] - lo]
            if packets:
                out.append((sid, packets))
        return out

    def _report_missing(self):
        """Periodically tell the sender what is still missing."""
        while not self.done.is_set():
            for sid, packets in self._missing():
                for i in range(0, len(packets), config.MISSING_MAX_PER_MSG):
                    try:
                        send_msg(self.sock, {
                            "type": "missing", "transfer_id": self.transfer_id,
                            "stream": sid,
                            "packets": packets[i:i + config.MISSING_MAX_PER_MSG]})
                    except OSError:
                        self.done.set()
                        return
            self.done.wait(config.MISSING_REPORT_INTERVAL)

    def _finish(self):
        if self.done.is_set():
            return
        self.done.set()
        try:
            os.close(self.fd)
        except OSError:
            pass
        digest = sha256_file(self.path)
        self.ok = (digest == self.expected_sha)
        self.detail = ("SHA-256 match" if self.ok else "SHA-256 MISMATCH")
        elapsed = max(time.time() - self.started_at, 0.001)
        try:
            send_msg(self.sock, {
                "type": "result", "transfer_id": self.transfer_id,
                "ok": self.ok, "sha256": digest, "detail": self.detail})
        except OSError:
            pass
        state.update_transfer(
            result="success" if self.ok else "failed",
            percent=100.0, done_bytes=self.meta["size"],
            speed_mbps=round(self.meta["size"] / elapsed / 1e6, 1),
            detail=self.detail + " -> " + self.path)
        state.log("Receive %s: %s (%s)"
                  % ("OK" if self.ok else "FAILED", self.meta["filename"], self.detail))
        self.sock.close()


# ---------------------------------------------------------------- engine

class TransferEngine:
    """Runs the TCP control server and the shared UDP data listener."""

    def __init__(self, identity, incoming_dir):
        self.identity = identity
        self.incoming_dir = incoming_dir
        self.stop_event = threading.Event()
        self.sessions = {}
        self.sessions_lock = threading.Lock()

    def start(self):
        threading.Thread(target=self._tcp_server, daemon=True).start()
        threading.Thread(target=self._udp_server, daemon=True).start()

    # ----- control (TCP) -----
    def _tcp_server(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("0.0.0.0", config.TCP_PORT))
        srv.listen(8)
        state.log("Control channel listening on TCP %d" % config.TCP_PORT)
        while not self.stop_event.is_set():
            try:
                conn, addr = srv.accept()
            except OSError:
                break
            threading.Thread(target=self._handle_control,
                             args=(conn, addr), daemon=True).start()

    def _handle_control(self, conn, addr):
        peer_ip = addr[0]
        try:
            while not self.stop_event.is_set():
                msg = recv_msg(conn)
                if msg is None:
                    break
                mtype = msg.get("type")
                if mtype == "HELLO":
                    name = str(msg.get("name", "unknown"))[:40]
                    state.set_connection("connected", peer_ip, name, "incoming")
                    with state._lock:
                        state.peers[peer_ip] = {
                            "name": name, "device_id": msg.get("device_id"),
                            "last_seen": time.time(),
                            "control_port": config.TCP_PORT}
                    send_msg(conn, {"type": "HELLO_ACK", "name": self.identity.name,
                                    "ip": self.identity.ip})
                    state.log("Connected with %s (%s)" % (name, peer_ip))
                elif mtype == "TRANSFER":
                    self._start_session(msg, conn)
                else:
                    break
        except (ConnectionResetError, BrokenPipeError, OSError):
            pass
        finally:
            try:
                conn.close()
            except OSError:
                pass
            if state.connection.get("peer_ip") == peer_ip:
                state.set_connection("idle")

    def _start_session(self, meta, conn):
        try:
            session = ReceiveSession(meta, conn, self.incoming_dir)
        except OSError as exc:
            send_msg(conn, {"type": "TRANSFER_ACK", "accepted": False,
                            "reason": str(exc)})
            return
        with self.sessions_lock:
            self.sessions[meta["transfer_id"]] = session
        send_msg(conn, {"type": "TRANSFER_ACK", "accepted": True})
        session.start()

    # ----- data (UDP) -----
    def _udp_server(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, config.SOCK_RCVBUF)
        try:
            s.bind(("0.0.0.0", config.UDP_DATA_PORT))
        except OSError as exc:
            state.log("UDP data bind failed: %s" % exc)
            return
        state.log("UDP data listener on port %d" % config.UDP_DATA_PORT)
        while not self.stop_event.is_set():
            try:
                data, addr = s.recvfrom(65535)
            except OSError:
                break
            # --- setup-time UDP handshake probe (JSON datagram) ---
            if data[:1] == b"{":
                try:
                    probe = json.loads(data.decode("utf-8"))
                except (ValueError, UnicodeDecodeError):
                    continue
                if probe.get("type") == "HELLO_PROBE":
                    with state._lock:
                        state.peers[addr[0]] = {
                            "name": str(probe.get("name", "unknown"))[:40],
                            "device_id": probe.get("device_id"),
                            "last_seen": time.time(),
                            "control_port": config.TCP_PORT}
                    reply = {"type": "HELLO_ACK_PROBE", "name": self.identity.name,
                             "device_id": self.identity.device_id}
                    try:
                        s.sendto(json.dumps(reply).encode("utf-8"), addr)
                    except OSError:
                        pass
                continue
            # --- framed file data ---
            parsed = protocol.parse_packet(data)
            if not parsed:
                continue
            tid, sid, pno, payload = parsed
            with self.sessions_lock:
                session = self.sessions.get(tid)
            if session:
                session.handle_packet(sid, pno, payload)

    def udp_probe(self, target_ip, timeout=3.0):
        """Setup-time UDP handshake: prove the UDP data path works.

        Returns the peer's HELLO_ACK_PROBE message, or None if the UDP
        path is unreachable (blocked by firewall/AP isolation).
        """
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.bind(("0.0.0.0", 0))
        s.settimeout(timeout)
        try:
            s.sendto(json.dumps({
                "type": "HELLO_PROBE", "name": self.identity.name,
                "device_id": self.identity.device_id,
            }).encode("utf-8"), (target_ip, config.UDP_DATA_PORT))
            data, _ = s.recvfrom(4096)
            msg = json.loads(data.decode("utf-8"))
            if msg.get("type") == "HELLO_ACK_PROBE":
                return msg
        except (OSError, ValueError):
            return None
        finally:
            s.close()

    def cleanup_session(self, transfer_id):
        with self.sessions_lock:
            self.sessions.pop(transfer_id, None)
