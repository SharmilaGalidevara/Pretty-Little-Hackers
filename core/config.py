# core/config.py  — all environment/configuration constants
import json, os

SIM_BASE     = "http://127.0.0.1:9898/api/v1"
SIM_USER     = "admin"
SIM_PASSWORD = "admin"

ENTRY_GATE   = "gateA"
EXIT_GATE    = "gateB"   # Verify physically — change to gateC if needed

WEB_PORT     = 8000
DB_PATH      = "parkmind.db"

# ── Game speed (must match GameSpeedMultiplier in simulator settings.json) ──
# If changed in settings.json, update this too so exit timers stay accurate.
GAME_SPEED_MULTIPLIER: float = 1.0

# Try to auto-read from simulator settings if the file is accessible
_SETTINGS_PATH = os.path.join(
    os.path.dirname(__file__),
    "..", "..",
    "ParkingSimulator-win-x64", "settings", "settings.json"
)
try:
    with open(_SETTINGS_PATH) as _f:
        _sim_cfg = json.load(_f)
        GAME_SPEED_MULTIPLIER = float(_sim_cfg.get("GameSpeedMultiplier", 1.0))
except Exception:
    pass  # Use default — fine for hackathon
