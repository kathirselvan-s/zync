"""Central configuration for the LAN file transfer app."""

# ----- Ports -----
TCP_PORT = 5005          # control channel (handshake, metadata, missing reports, result)
UDP_DATA_PORT = 5007     # bulk file data
DISCOVERY_PORT = 5006    # peer broadcast beacons

# ----- Transfer tuning -----
CHUNK_SIZE = 60 * 1024   # 60 KB per UDP datagram (safe from IP fragmentation)
STREAM_COUNT = 5         # parallel streams / threads
MISSING_REPORT_INTERVAL = 0.4   # seconds between missing-packet reports
MISSING_MAX_PER_MSG = 400       # max packet numbers per MISSING message
TRANSFER_TIMEOUT = 3600         # overall transfer timeout (seconds)
SOCK_RCVBUF = 4 * 1024 * 1024   # enlarge UDP receive buffer

# ----- Storage -----
RECEIVE_DIR = "received"
