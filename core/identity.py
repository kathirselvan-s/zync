"""Own device identity: local IP, user-chosen computer name, device id.

The name persists to disk so it survives restarts of the EXE.
"""
import json
import os
import uuid
from pathlib import Path

from .netutil import get_local_ip

_CONFIG_DIR = Path(os.environ.get("LFT_HOME", Path.home() / ".lan_file_transfer"))
_CONFIG_DIR.mkdir(parents=True, exist_ok=True)
_IDENTITY_FILE = _CONFIG_DIR / "identity.json"


class Identity:
    def __init__(self):
        self.device_id = None
        self.name = None
        self._load()
        self.ip = get_local_ip()          # refreshed on demand
        self.control_port = None          # set by app at startup

    def _load(self):
        try:
            data = json.loads(_IDENTITY_FILE.read_text("utf-8"))
            self.device_id = data["device_id"]
            self.name = data["name"]
        except (OSError, ValueError, KeyError):
            self.device_id = uuid.uuid4().hex
            self.name = "PC-" + self.device_id[:6].upper()
            self._save()

    def _save(self):
        try:
            _IDENTITY_FILE.write_text(json.dumps({
                "device_id": self.device_id,
                "name": self.name,
            }), "utf-8")
        except OSError:
            pass

    def set_name(self, name):
        name = (name or "").strip()[:40]
        if name:
            self.name = name
            self._save()

    def refresh_ip(self):
        self.ip = get_local_ip()
        return self.ip

    def beacon_payload(self):
        return {
            "type": "beacon",
            "name": self.name,
            "device_id": self.device_id,
            "ip": self.ip,
            "control_port": self.control_port,
        }
