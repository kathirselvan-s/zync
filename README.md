# LAN File Transfer (Python + Flask + reliable UDP)

Desktop app for fast file transfer between two laptops on the same LAN
(e.g. 192.168.1.50 <-> 192.168.1.36). Ships as a single EXE.

## Which protocol is used when (setup time)

| Phase | Protocol | Port | Purpose |
|---|---|---|---|
| Device discovery | **UDP** broadcast | 5006 | beacons with name + IP |
| Setup handshake probe | **UDP** | 5007 | proves the UDP data path works before connecting |
| Control channel | **TCP** | 5005 | HELLO, file metadata, missing-packet reports, result |
| File data | **UDP**, 5 parallel streams | 5007 | 60 KB packets, CRC32 + retransmit + SHA-256 |

The app always tells you which path is used: the dashboard shows
`setup probe UDP ✓/✗ · control TCP · data UDP` under Connection.

## Features (backend)
- Shows **your IP** and lets you set a **computer name** (persisted).
- **Auto-discovers** other devices on the LAN (UDP broadcast beacons).
- **Connect** to a discovered or manually typed IP (TCP handshake).
- **Send / Receive** any file:
  - file split into 60 KB UDP packets, 5 parallel streams (threads)
  - per-packet header + CRC32; corrupt packets are discarded and re-requested
  - receiver tracks a per-stream bitmap, reports missing packets,
    sender retransmits only those
  - receiver writes chunks straight into a preallocated file (no RAM bloat)
  - final **SHA-256** verification before "success" is declared
- Web dashboard (Flask) at http://127.0.0.1:5000 with progress/speed/log.

## Run from source
    pip install -r requirements.txt
    python app.py
Open http://127.0.0.1:5000 in a browser. Run on both laptops.

## Build the EXE (Windows)
    pip install pyinstaller
    pyinstaller --onefile --name LanFileTransfer --add-data "templates;templates" app.py
    dist\LanFileTransfer.exe
The EXE starts the backend and serves the dashboard on http://127.0.0.1:5000.
Received files land in the `received` folder next to the EXE (run it from a
folder you can find, not a temp dir). Allow the firewall prompt for
TCP 5005 and UDP 5006/5007.

## API (used by the UI)
    GET  /api/identity            own IP + name
    POST /api/identity            set name        {"name": "..."}
    GET  /api/status              peers, connection, transfer progress, log
    POST /api/connect             connect        {"ip": "192.168.1.36"}
    POST /api/disconnect
    POST /api/send                send file      {"path": "C:\\...\\file.iso"}
    POST /api/cancel

## Notes / limits
- V1 targets the **same LAN**. Internet/NAT traversal is stage V6
  (needs port forwarding / STUN / relay) — same data protocol carries over.
- Both laptops must allow inbound TCP 5005 + UDP 5006,5007 in the firewall
  (Windows will prompt the first time — click Allow).
- Broadcast discovery can be blocked on some Wi-Fi/AP setups; the
  "type IP manually" connect option always works.
