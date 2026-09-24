"""UDP data-packet framing: header + CRC32 per packet.

Header layout (22 bytes, network byte order):
    magic      4s   b"LFT1"
    transfer   Q    transfer id
    stream     H    stream id (0..N-1)
    packet_no  I    packet number within the stream
    payload_len I   length of data following the header
    crc32      I    CRC32 of payload
"""
import struct
import zlib

MAGIC = b"LFT1"
HEADER = struct.Struct("!4sQHIII")
HEADER_SIZE = HEADER.size


def build_packet(transfer_id, stream_id, packet_no, payload):
    crc = zlib.crc32(payload) & 0xFFFFFFFF
    return HEADER.pack(MAGIC, transfer_id, stream_id, packet_no, len(payload), crc) + payload


def parse_packet(data):
    """Return (transfer_id, stream_id, packet_no, payload) or None if invalid."""
    if len(data) < HEADER_SIZE:
        return None
    magic, tid, sid, pno, plen, crc = HEADER.unpack_from(data)
    if magic != MAGIC:
        return None
    payload = data[HEADER_SIZE:HEADER_SIZE + plen]
    if len(payload) != plen:
        return None
    if (zlib.crc32(payload) & 0xFFFFFFFF) != crc:
        return None
    return tid, sid, pno, payload
