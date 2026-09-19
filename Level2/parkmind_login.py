from flask import Flask, request, jsonify, render_template_string, redirect, url_for, session
import requests
import sqlite3
import threading
import hashlib
import json
import math
import os
import re
import time
from pathlib import Path
from datetime import datetime, timedelta
from urllib.parse import quote
from collections import defaultdict

# ============================================================
# PARKMIND LEVEL 2 — CORE + LOGIN + WEBHOOK CONTROLLER
# ============================================================
# This is the ONE file you run.
# It imports the Admin, Operator and Maintenance dashboard files.
#
# IMPORTANT:
# - Operational timestamps/durations come from simulator ServerDateTime.
# - Revenue comes only from accepted payment_made simulator events.
# - CO values come only from carbon_monoxide_event simulator events.
# - Component inventory/state starts from simulator list-* APIs.
# - Level 2 acts ONLY on valid signed webhooks. Unsigned/invalid webhooks are logged.
# ============================================================

APP_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.getenv("PARKMIND_LEVEL2_DB", str(APP_DIR / "parkmind_level2.db")))

SIM_BASE = os.getenv("PARKMIND_SIM_BASE", "http://127.0.0.1:9898/api/v1")
SIM_USER = os.getenv("PARKMIND_SIM_USER", "admin")
SIM_PASSWORD = os.getenv("PARKMIND_SIM_PASSWORD", "admin")
WEB_PORT = int(os.getenv("PARKMIND_PORT", "8000"))

DAY_START_HOUR = int(os.getenv("PARKMIND_DAY_START", "6"))
DAY_END_HOUR = int(os.getenv("PARKMIND_DAY_END", "18"))

# Configured tariff used to calculate the amount sent to the simulator.
# Durations themselves always come from simulator timestamps.
PARKING_RATE_PER_MIN = float(os.getenv("PARKMIND_PARKING_RATE", "1"))
ELECTRIC_TOTAL_MULTIPLIER = float(os.getenv("PARKMIND_ELECTRIC_TOTAL_MULTIPLIER", "2"))

app = Flask(__name__)
app.secret_key = os.getenv(
    "PARKMIND_SECRET",
    "pretty-little-hackers-level2-shared-session"
)

USERS = {
    "admin": {"password": "admin", "role": "Admin"},
    "operator": {"password": "operator", "role": "Operator"},
    "maintenance": {"password": "maintenance", "role": "Maintenance"},
}

token = None
token_lock = threading.Lock()
state_lock = threading.RLock()

last_sequence_id = None
last_sequence_lock = threading.Lock()

entry_queues = defaultdict(list)          # gate -> [{plate, spot}]
entry_active = {}                         # gate -> {plate, spot, sent}
paid_exit_queues = defaultdict(list)      # gate -> [PAID plates ready for gate release]
exit_active = {}                          # gate -> {plate, sent}

# Physical queue at each REAL ExitSpot sensor.
# This follows the supplied reference payment method, upgraded for Level 2:
# cars may physically queue at an exit, but ONLY the front car can be released.
physical_exit_queues = defaultdict(list)  # ExitSpot name -> [plate, plate, ...]

# Cars currently travelling into each real API-reported parking zone.
# Key = Park bay zoneParent. No zone names are hardcoded.
zone_access_users = defaultdict(set)

exit_request_queue = []                   # parked cars waiting to approach an API ExitSpot
scheduled_exit_plates = set()

# Level-1-compatible physical flow protection for Level 2.
# We learn which barrier belongs to each EntrySpot / ExitSpot from real
# simulator topology + successful physical crossings. No hard-coded gate names.
ROUTE_RELEASE_FALLBACK_SEC = 0.80
LANE_DISCOVERY_TIMEOUT_SEC = 4.00
ENTRY_IDLE_RESTORE_GRACE_SEC = 1.5
EXIT_PAYMENT_RETRY_SEC = 2.0

# Software reservations protect a bay while a car travels from EntrySpot to
# the actual parking sensor. They must not survive forever after a failed route
# or simulator restart.
RESERVATION_TTL_SECONDS = 45

lane_trial_lock = threading.RLock()
topology_sync_lock = threading.RLock()

# First /list-barriers snapshot seen in THIS PARKMIND process.
# This is API-derived, not hardcoded. It protects simulator road topology:
# a barrier that the simulator loaded Open is never auto-closed by discovery.
gate_initial_api_state = {}

# ============================================================
# REAL LEVEL 2 TOPOLOGY — API-ONLY
# ============================================================
# The simulator API proves:
# - parking/entry/exit sensor purpose from /list-parking-spots
# - parking/exit zoneParent
# - barrier name, zoneParent, state, broken, isUnderMaintenance
#
# It does NOT expose a barrier role (entry/exit) and EntrySpot rows do not
# expose a zone association. PARKMIND therefore does not invent those fields.

LOGIN_HTML = """
<!doctype html>
<html><head><title>PARKMIND Level 2 — Login</title>
<style>
*{box-sizing:border-box}
body{margin:0;font-family:'Segoe UI',Arial;background:#07111f;color:#e5e7eb;min-height:100vh;display:grid;place-items:center}
.card{width:390px;background:#101b2d;border:1px solid #23314a;border-radius:20px;padding:30px;box-shadow:0 24px 80px rgba(0,0,0,.45)}
.logo{font-size:29px;font-weight:900;color:#67e8f9}.sub{color:#94a3b8;font-size:12px;margin:3px 0 20px}
label{display:block;font-size:11px;color:#94a3b8;margin-top:10px}
input,button{width:100%;padding:12px;border-radius:9px;margin-top:5px}
input{background:#08111f;color:white;border:1px solid #334155}
button{border:0;background:#0284c7;color:white;font-weight:800;cursor:pointer;margin-top:14px}
.err{background:#3f151a;color:#fecaca;border:1px solid #7f1d1d;padding:9px;border-radius:8px;font-size:12px}
.roles{margin-top:18px;padding:10px;background:#0b1424;border-radius:10px;font-size:11px;color:#94a3b8;line-height:1.6}
</style></head>
<body><div class="card">
<div class="logo">PARKMIND</div>
<div class="sub">Level 2 · Role-Based Control Platform</div>
{% if error %}<div class="err">{{error}}</div>{% endif %}
<form method="post">
<label>Name</label><input name="username" required autofocus placeholder="admin / operator / maintenance">
<label>Password</label><input name="password" type="password" required>
<button>Secure Login</button>
</form>
<div class="roles">
Admin: <b>admin / admin</b><br>
Operator: <b>operator / operator</b><br>
Maintenance: <b>maintenance / maintenance</b>
</div>
</div></body></html>
"""


# ============================================================
# DATABASE
# ============================================================
def db():
    conn = sqlite3.connect(str(DB_PATH), timeout=15)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = db()
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS system_state(
        key TEXT PRIMARY KEY,
        value TEXT
    );

    CREATE TABLE IF NOT EXISTS events(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id TEXT UNIQUE,
        sequence_id INTEGER,
        event_class TEXT,
        server_time TEXT,
        signature_status TEXT,
        payload TEXT
    );

    CREATE TABLE IF NOT EXISTS cars(
        plate TEXT PRIMARY KEY,
        car_type TEXT,
        planned_minutes INTEGER DEFAULT 0,
        entry_time TEXT,
        entry_spot TEXT,
        entry_zone TEXT,
        assigned_spot TEXT,
        actual_spot TEXT,
        parked_time TEXT,
        exit_arrival_time TEXT,
        exit_spot TEXT,
        exit_zone TEXT,
        exit_gate TEXT,
        departure_time TEXT,
        total_stay_seconds INTEGER DEFAULT 0,
        total_stay_minutes INTEGER DEFAULT 0,
        billable_seconds INTEGER DEFAULT 0,
        billable_minutes INTEGER DEFAULT 0,
        parking_cost REAL DEFAULT 0,
        charging_cost REAL DEFAULT 0,
        expected_amount REAL DEFAULT 0,
        actual_paid REAL DEFAULT 0,
        payment_time TEXT,
        payment_status TEXT DEFAULT 'NONE',
        status TEXT DEFAULT 'NEW',
        decision TEXT
    );

    CREATE TABLE IF NOT EXISTS reservations(
        spot TEXT PRIMARY KEY,
        plate TEXT,
        reserved_at TEXT
    );

    CREATE TABLE IF NOT EXISTS components(
        name TEXT,
        kind TEXT,
        zone TEXT,
        state TEXT,
        broken INTEGER DEFAULT 0,
        under_maintenance INTEGER DEFAULT 0,
        cycles INTEGER DEFAULT 0,
        runtime_seconds INTEGER DEFAULT 0,
        on_since TEXT,
        maintenance_required INTEGER DEFAULT 0,
        maintenance_problem TEXT,
        api_life_pct REAL,
        api_life_source TEXT,
        api_usage_current REAL,
        api_usage_limit REAL,
        api_runtime_hours REAL,
        api_runtime_limit_hours REAL,
        last_alarm_time TEXT,
        last_event_time TEXT,
        raw_json TEXT,
        PRIMARY KEY(name, kind)
    );

    CREATE TABLE IF NOT EXISTS gate_roles(
        name TEXT PRIMARY KEY,
        role TEXT,
        zone TEXT,
        source TEXT
    );

    CREATE TABLE IF NOT EXISTS lane_map(
        sensor TEXT,
        role TEXT,
        gate TEXT,
        confidence TEXT,
        source TEXT,
        updated_at TEXT,
        PRIMARY KEY(sensor, role)
    );

    CREATE TABLE IF NOT EXISTS zones(
        name TEXT PRIMARY KEY,
        co_level REAL,
        danger TEXT,
        last_update TEXT
    );

    CREATE TABLE IF NOT EXISTS alerts(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        alert_key TEXT UNIQUE,
        created_at TEXT,
        simulator_time TEXT,
        severity TEXT,
        alert_type TEXT,
        plate TEXT,
        zone TEXT,
        component TEXT,
        reason TEXT,
        active INTEGER DEFAULT 1,
        resolved_at TEXT
    );

    CREATE TABLE IF NOT EXISTS penalties(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        simulator_time TEXT,
        reason TEXT,
        fine_amount REAL,
        plate TEXT,
        payload TEXT
    );

    CREATE TABLE IF NOT EXISTS login_attempts(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        attempted_at TEXT,
        username TEXT,
        success INTEGER,
        ip TEXT
    );

    CREATE TABLE IF NOT EXISTS audit_log(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT,
        simulator_time TEXT,
        actor TEXT,
        action TEXT,
        target TEXT,
        detail TEXT,
        result TEXT
    );

    CREATE TABLE IF NOT EXISTS maintenance_actions(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT,
        simulator_time TEXT,
        actor TEXT,
        component TEXT,
        kind TEXT,
        reason TEXT,
        action TEXT,
        result TEXT,
        repair_type TEXT,
        status TEXT DEFAULT 'PENDING',
        completed_simulator_time TEXT,
        repair_duration_seconds INTEGER DEFAULT 0,
        repair_cost REAL,
        cost_source TEXT
    );
    """)

    # Safe car migrations for databases created by earlier Level 2 builds.
    car_cols = {r[1] for r in conn.execute("PRAGMA table_info(cars)").fetchall()}
    car_migrations = {
        "total_stay_seconds": "INTEGER DEFAULT 0",
        "total_stay_minutes": "INTEGER DEFAULT 0",
    }
    for col, ddl in car_migrations.items():
        if col not in car_cols:
            conn.execute(f"ALTER TABLE cars ADD COLUMN {col} {ddl}")

    # Safe migrations for databases created by earlier Level 2 builds.
    component_cols = {r[1] for r in conn.execute("PRAGMA table_info(components)").fetchall()}
    component_migrations = {
        "maintenance_required": "INTEGER DEFAULT 0",
        "maintenance_problem": "TEXT",
        "api_life_pct": "REAL",
        "api_life_source": "TEXT",
        "api_usage_current": "REAL",
        "api_usage_limit": "REAL",
        "api_runtime_hours": "REAL",
        "api_runtime_limit_hours": "REAL",
        "last_alarm_time": "TEXT",
    }
    for col, ddl in component_migrations.items():
        if col not in component_cols:
            conn.execute(f"ALTER TABLE components ADD COLUMN {col} {ddl}")

    action_cols = {r[1] for r in conn.execute("PRAGMA table_info(maintenance_actions)").fetchall()}
    action_migrations = {
        "repair_type": "TEXT",
        "status": "TEXT DEFAULT 'PENDING'",
        "completed_simulator_time": "TEXT",
        "repair_duration_seconds": "INTEGER DEFAULT 0",
        "repair_cost": "REAL",
        "cost_source": "TEXT",
    }
    for col, ddl in action_migrations.items():
        if col not in action_cols:
            conn.execute(f"ALTER TABLE maintenance_actions ADD COLUMN {col} {ddl}")

    conn.commit()
    conn.close()


def set_state(key, value):
    conn = db()
    conn.execute(
        "INSERT INTO system_state(key,value) VALUES(?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, str(value) if value is not None else "")
    )
    conn.commit()
    conn.close()


def get_state(key, default=""):
    conn = db()
    row = conn.execute("SELECT value FROM system_state WHERE key=?", (key,)).fetchone()
    conn.close()
    return row["value"] if row else default


def simulator_now():
    return get_state("last_simulator_time", "")


def parse_sim_time(value):
    if not value:
        return None
    try:
        return datetime.strptime(str(value)[:19], "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def sim_seconds_between(start, end):
    a, b = parse_sim_time(start), parse_sim_time(end)
    if not a or not b:
        return 0
    return max(0, int((b - a).total_seconds()))


def fmt_duration(seconds):
    seconds = max(0, int(seconds or 0))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m:02d}m {s:02d}s"
    if m:
        return f"{m}m {s:02d}s"
    return f"{s}s"


def audit(actor, action, target="", detail="", result="OK", sim_time=None):
    conn = db()
    conn.execute(
        """INSERT INTO audit_log(
            created_at, simulator_time, actor, action, target, detail, result
        ) VALUES(?,?,?,?,?,?,?)""",
        (
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            sim_time or simulator_now() or None,
            actor,
            action,
            target,
            detail,
            result
        )
    )
    conn.commit()
    conn.close()


def record_login(username, success):
    conn = db()
    conn.execute(
        "INSERT INTO login_attempts(attempted_at,username,success,ip) VALUES(?,?,?,?)",
        (
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            username,
            1 if success else 0,
            request.remote_addr or ""
        )
    )
    conn.commit()
    conn.close()


def upsert_car(plate, **fields):
    if not plate:
        return
    conn = db()
    conn.execute("INSERT OR IGNORE INTO cars(plate) VALUES(?)", (plate,))
    if fields:
        cols = ", ".join(f"{k}=?" for k in fields)
        vals = list(fields.values()) + [plate]
        conn.execute(f"UPDATE cars SET {cols} WHERE plate=?", vals)
    conn.commit()
    conn.close()


def get_car(plate):
    conn = db()
    row = conn.execute("SELECT * FROM cars WHERE plate=?", (plate,)).fetchone()
    conn.close()
    return dict(row) if row else None


def upsert_alert(key, severity, alert_type, reason, sim_time=None,
                 plate="", zone="", component=""):
    conn = db()
    conn.execute(
        """INSERT INTO alerts(
            alert_key, created_at, simulator_time, severity, alert_type,
            plate, zone, component, reason, active
        ) VALUES(?,?,?,?,?,?,?,?,?,1)
        ON CONFLICT(alert_key) DO UPDATE SET
            simulator_time=excluded.simulator_time,
            severity=excluded.severity,
            alert_type=excluded.alert_type,
            plate=excluded.plate,
            zone=excluded.zone,
            component=excluded.component,
            reason=excluded.reason,
            active=1,
            resolved_at=NULL""",
        (
            key,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            sim_time or simulator_now() or None,
            severity,
            alert_type,
            plate or "",
            zone or "",
            component or "",
            reason
        )
    )
    conn.commit()
    conn.close()


def resolve_alert(key, sim_time=None):
    conn = db()
    conn.execute(
        "UPDATE alerts SET active=0,resolved_at=? WHERE alert_key=?",
        (sim_time or simulator_now() or datetime.now().strftime("%Y-%m-%d %H:%M:%S"), key)
    )
    conn.commit()
    conn.close()


# ============================================================
# SIMULATOR API
# ============================================================
def sim_login():
    global token
    with token_lock:
        r = requests.post(
            f"{SIM_BASE}/auth/login",
            json={"email": SIM_USER, "password": SIM_PASSWORD},
            timeout=7
        )
        r.raise_for_status()
        body = r.json()
        token = body.get("token") or body.get("accessToken") or body.get("access_token")
        if not token:
            raise RuntimeError("Simulator login returned no JWT token")
        return token


def sim_request(method, path, **kwargs):
    global token
    if not token:
        sim_login()
    headers = kwargs.pop("headers", {})
    headers["Authorization"] = f"Bearer {token}"

    for attempt in range(3):
        try:
            r = requests.request(
                method,
                f"{SIM_BASE}{path}",
                headers=headers,
                timeout=8,
                **kwargs
            )
            if r.status_code == 401:
                sim_login()
                headers["Authorization"] = f"Bearer {token}"
                continue
            if r.status_code >= 500:
                time.sleep(2 ** attempt)
                continue
            r.raise_for_status()
            return r
        except requests.RequestException:
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)
    raise RuntimeError(f"Simulator API failed: {method} {path}")


def detected_count(value):
    if isinstance(value, list):
        return len(value)
    try:
        return int(value or 0)
    except Exception:
        return 0


def _num(value):
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def _first_ci(item, keys):
    lowered = {str(k).lower(): v for k, v in (item or {}).items()}
    for key in keys:
        if key.lower() in lowered:
            return lowered[key.lower()]
    return None


def extract_simulator_maintenance_metrics(item):
    """
    Only uses fields returned by the simulator API.

    If the simulator exposes a direct life/health percentage, use it.
    Otherwise, a remaining-life percentage is calculated ONLY when the
    simulator itself exposes both current usage and its limit, or both
    runtime and its limit. If neither exists, life remains None.
    """
    direct_life = _num(_first_ci(item, [
        "remainingLifePercent", "remainingLifePercentage",
        "lifePercent", "lifePercentage",
        "healthPercent", "healthPercentage"
    ]))

    usage_current = _num(_first_ci(item, [
        "usageCount", "usedCycles", "cycleCount", "cycles",
        "currentCycles", "workCycles"
    ]))
    usage_limit = _num(_first_ci(item, [
        "maxUsageCount", "maxCycles", "cycleLimit", "usageLimit",
        "maintenanceAfterCycles", "repairAfterCycles"
    ]))

    runtime_hours = _num(_first_ci(item, [
        "usageHours", "runningHours", "runtimeHours",
        "workingHours", "workedHours"
    ]))
    runtime_limit_hours = _num(_first_ci(item, [
        "maxUsageHours", "maxRunningHours", "runtimeLimitHours",
        "maintenanceAfterHours", "repairAfterHours"
    ]))

    candidates = []
    source_parts = []

    if direct_life is not None:
        candidates.append(max(0.0, min(100.0, direct_life)))
        source_parts.append("API percentage")

    if usage_current is not None and usage_limit and usage_limit > 0:
        remaining = 100.0 * (1.0 - usage_current / usage_limit)
        candidates.append(max(0.0, min(100.0, remaining)))
        source_parts.append("API cycles/limit")

    if runtime_hours is not None and runtime_limit_hours and runtime_limit_hours > 0:
        remaining = 100.0 * (1.0 - runtime_hours / runtime_limit_hours)
        candidates.append(max(0.0, min(100.0, remaining)))
        source_parts.append("API runtime/limit")

    life = min(candidates) if candidates else None
    return {
        "api_life_pct": round(life, 1) if life is not None else None,
        "api_life_source": " + ".join(source_parts) if source_parts else None,
        "api_usage_current": usage_current,
        "api_usage_limit": usage_limit,
        "api_runtime_hours": runtime_hours,
        "api_runtime_limit_hours": runtime_limit_hours,
    }


def upsert_component(kind, item, sim_time=None):
    name = str(item.get("name") or item.get("Name") or "").strip()
    if not name:
        return

    zone = str(item.get("zoneParent") or item.get("ZoneName") or "").strip()
    state = item.get("state") or item.get("State") or ""

    if kind == "spot":
        purpose = str(item.get("purpose") or item.get("SpotType") or "")
        if purpose == "Park":
            state = "Occupied" if detected_count(item.get("detectedCars")) > 0 else "Free"
        else:
            state = purpose or state or "Sensor"
    elif kind in ("fan", "light") and "isOn" in item:
        state = "On" if bool(item.get("isOn")) else "Off"

    broken = 1 if item.get("broken", False) else 0
    under = 1 if item.get("isUnderMaintenance", False) else 0
    api_metrics = extract_simulator_maintenance_metrics(item)

    conn = db()
    existing = conn.execute(
        "SELECT cycles,runtime_seconds,on_since FROM components WHERE name=? AND kind=?",
        (name, kind)
    ).fetchone()
    cycles = int(existing["cycles"] or 0) if existing else 0
    runtime = int(existing["runtime_seconds"] or 0) if existing else 0
    on_since = existing["on_since"] if existing else None

    conn.execute(
        """INSERT INTO components(
            name,kind,zone,state,broken,under_maintenance,cycles,
            runtime_seconds,on_since,
            api_life_pct,api_life_source,api_usage_current,api_usage_limit,
            api_runtime_hours,api_runtime_limit_hours,
            last_event_time,raw_json
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(name,kind) DO UPDATE SET
            zone=excluded.zone,
            state=excluded.state,
            broken=excluded.broken,
            under_maintenance=excluded.under_maintenance,
            api_life_pct=excluded.api_life_pct,
            api_life_source=excluded.api_life_source,
            api_usage_current=excluded.api_usage_current,
            api_usage_limit=excluded.api_usage_limit,
            api_runtime_hours=excluded.api_runtime_hours,
            api_runtime_limit_hours=excluded.api_runtime_limit_hours,
            last_event_time=excluded.last_event_time,
            raw_json=excluded.raw_json""",
        (
            name, kind, zone, str(state), broken, under,
            cycles, runtime, on_since,
            api_metrics["api_life_pct"],
            api_metrics["api_life_source"],
            api_metrics["api_usage_current"],
            api_metrics["api_usage_limit"],
            api_metrics["api_runtime_hours"],
            api_metrics["api_runtime_limit_hours"],
            sim_time or simulator_now() or None,
            json.dumps(item)
        )
    )
    conn.commit()
    conn.close()


def _purpose_from_component_row(row):
    try:
        raw = json.loads(row["raw_json"] or "{}")
    except Exception:
        raw = {}
    return str(raw.get("purpose") or "")


def topology_counts():
    conn = db()
    rows = conn.execute(
        "SELECT name,raw_json FROM components WHERE kind='spot'"
    ).fetchall()
    gates = conn.execute(
        "SELECT COUNT(*) AS n FROM components WHERE kind='gate'"
    ).fetchone()["n"]
    zone_count = conn.execute(
        "SELECT COUNT(*) AS n FROM zones"
    ).fetchone()["n"]
    conn.close()

    park = entry = exit_spot = leave = 0
    for row in rows:
        try:
            raw = json.loads(row["raw_json"] or "{}")
        except Exception:
            raw = {}
        purpose = str(raw.get("purpose") or "")
        if purpose == "Park":
            park += 1
        elif purpose == "EntrySpot":
            entry += 1
        elif purpose == "ExitSpot":
            exit_spot += 1
        elif purpose == "LeaveParking":
            leave += 1

    return {
        "park": park,
        "entry": entry,
        "exit": exit_spot,
        "leave": leave,
        "gates": int(gates or 0),
        "zones": int(zone_count or 0),
    }


def level2_topology_ready():
    """
    Dynamic readiness check.

    No fixed number of zones, bays, gates, EntrySpots or ExitSpots is assumed.
    PARKMIND is ready when the simulator API has exposed the component classes
    required for normal parking flow.
    """
    counts = topology_counts()
    return (
        counts["park"] > 0
        and counts["entry"] > 0
        and counts["exit"] > 0
        and counts["gates"] > 0
        and counts["zones"] > 0
    )


def ensure_level2_topology_ready(reason="runtime"):
    """
    Makes Level 2 robust to startup order.

    PARKMIND may be started before the Level 2 map is loaded. The first signed
    EntrySpot/ExitSpot event proves the simulator is live, so at that point we
    perform one defensive topology sync if the required component classes have
    not yet been discovered.
    """
    if level2_topology_ready():
        return True

    with topology_sync_lock:
        if level2_topology_ready():
            return True
        sync_topology(f"lazy:{reason}")
        return level2_topology_ready()


def prune_stale_components(loaded):
    """Remove components left over from another simulator level."""
    conn = db()
    for kind in ("spot", "gate", "light", "fan", "alarm"):
        if kind == "alarm":
            continue
        names = [
            str(item.get("name") or item.get("Name") or "").strip()
            for item in loaded.get(kind, [])
            if str(item.get("name") or item.get("Name") or "").strip()
        ]
        if names:
            placeholders = ",".join("?" for _ in names)
            conn.execute(
                f"DELETE FROM components WHERE kind=? AND name NOT IN ({placeholders})",
                [kind] + names
            )
        else:
            conn.execute("DELETE FROM components WHERE kind=?", (kind,))

    # Remove learned mappings that point to barriers no longer in this level.
    conn.execute(
        """DELETE FROM lane_map
           WHERE gate NOT IN(
             SELECT name FROM components WHERE kind='gate'
           )"""
    )
    conn.execute(
        """DELETE FROM gate_roles
           WHERE name NOT IN(
             SELECT name FROM components WHERE kind='gate'
           )"""
    )
    conn.commit()
    conn.close()


def apply_alarm_snapshot(alarms, reason="api", sim_time=None):
    sim_time = sim_time or simulator_now() or None
    names = set()
    conn = db()

    # Clear only the simulator-maintenance recommendation flag.
    conn.execute(
        """UPDATE components
           SET maintenance_required=0, maintenance_problem=NULL
           WHERE broken=0 AND under_maintenance=0"""
    )

    for alarm in alarms or []:
        name = str(alarm.get("name") or alarm.get("Name") or "").strip()
        problem = str(alarm.get("problem") or alarm.get("Problem") or "Require Maintenance").strip()
        if not name:
            continue
        names.add(name)
        cur = conn.execute(
            """UPDATE components
               SET maintenance_required=1,
                   maintenance_problem=?,
                   last_alarm_time=?
               WHERE name=?""",
            (problem, sim_time, name)
        )
        if cur.rowcount:
            conn.execute(
                """INSERT INTO alerts(
                    alert_key,created_at,simulator_time,severity,alert_type,
                    plate,zone,component,reason,active
                )
                SELECT ?,?,?,?,?, '',zone,name,?,1
                FROM components WHERE name=? LIMIT 1
                ON CONFLICT(alert_key) DO UPDATE SET
                    simulator_time=excluded.simulator_time,
                    severity=excluded.severity,
                    alert_type=excluded.alert_type,
                    zone=excluded.zone,
                    component=excluded.component,
                    reason=excluded.reason,
                    active=1,
                    resolved_at=NULL""",
                (
                    f"MAINT_DUE:{name}",
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    sim_time,
                    "HIGH",
                    "SIMULATOR MAINTENANCE REQUIRED",
                    problem,
                    name
                )
            )

    # Resolve old due alerts that are no longer in the authoritative alarm snapshot.
    due_rows = conn.execute(
        "SELECT name FROM components WHERE maintenance_required=0"
    ).fetchall()
    for row in due_rows:
        conn.execute(
            "UPDATE alerts SET active=0,resolved_at=? WHERE alert_key=?",
            (sim_time or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
             f"MAINT_DUE:{row['name']}")
        )

    conn.commit()
    conn.close()
    audit("system", "MAINTENANCE_ALARM_SYNC", reason, f"alarms={len(alarms or [])}", "OK", sim_time)


def refresh_maintenance_snapshot(reason="manual"):
    """
    Manual/cost-aware maintenance refresh.

    The simulator documentation discourages periodic list-* polling, so this is
    NOT a background poll. It is called at level load or explicitly by an
    authorized Admin/Maintenance user.
    """
    sync_topology(reason)


def capture_gate_api_baseline(barrier_items):
    """
    Capture the simulator's first API-reported barrier state for this run.

    /list-barriers does not expose road connectivity or entry/exit roles.
    Therefore PARKMIND preserves the simulator's own initial Open/Closed state
    instead of assuming an unknown gate is safe to close.
    """
    with state_lock:
        for item in barrier_items or []:
            name = str(item.get("name") or item.get("Name") or "").strip()
            if not name or name in gate_initial_api_state:
                continue

            state = str(item.get("state") or item.get("State") or "").strip()
            if state:
                gate_initial_api_state[name] = state

    if barrier_items:
        snapshot = ", ".join(
            f"{name}={state}"
            for name, state in sorted(gate_initial_api_state.items())
        )
        print(f"[GATE API BASELINE] {snapshot}")


def gate_api_baseline(name):
    with state_lock:
        return str(gate_initial_api_state.get(name) or "")


def auto_restore_gate_api_baseline(name, actor="system", sim_time=None):
    """
    Restore only the state proved by the FIRST /list-barriers snapshot.

    Crucially:
      - baseline Open  -> PARKMIND will NOT auto-close it
      - baseline Closed -> PARKMIND may close it after temporary use

    This prevents automatic lane-discovery cleanup from cutting an unknown
    simulator road and causing 'No path from A to P2'.
    """
    baseline = gate_api_baseline(name)
    row = component_row(name, "gate") or {}
    current = str(row.get("state") or "")

    if not baseline:
        audit(
            actor,
            "GATE_BASELINE_SKIP",
            name,
            "No initial API state was captured; automatic state change skipped.",
            "SKIPPED",
            sim_time or simulator_now()
        )
        return False

    base = baseline.lower()
    cur = current.lower()

    if base in ("open", "opening"):
        # Never auto-close a gate that the simulator initially exposed Open.
        if cur not in ("open", "opening"):
            component_action("gate", name, "open", actor, sim_time)
        else:
            audit(
                actor,
                "GATE_BASELINE_PRESERVED",
                name,
                f"Initial API state={baseline}; leaving gate open.",
                "OK",
                sim_time or simulator_now()
            )
        return True

    if base in ("closed", "closing"):
        if cur not in ("closed", "closing"):
            component_action("gate", name, "close", actor, sim_time)
        return True

    audit(
        actor,
        "GATE_BASELINE_SKIP",
        name,
        f"Unsupported initial API state={baseline!r}; automatic state change skipped.",
        "SKIPPED",
        sim_time or simulator_now()
    )
    return False


def sync_topology(reason="startup"):
    endpoints = {
        "spot": "/list-parking-spots",
        "gate": "/list-barriers",
        "light": "/list-lights",
        "fan": "/list-exhaust-fans",
        "alarm": "/list-alarms",
    }

    loaded = {}
    for kind, endpoint in endpoints.items():
        try:
            data = sim_request("GET", endpoint).json()
            loaded[kind] = data if isinstance(data, list) else []
            if kind != "alarm":
                for item in loaded[kind]:
                    upsert_component(kind, item)
        except Exception as e:
            loaded[kind] = []
            audit("system", "TOPOLOGY_SYNC_ERROR", kind, str(e), "ERROR")

    # Preserve the simulator's own barrier topology state before PARKMIND
    # performs any automatic gate operation.
    capture_gate_api_baseline(loaded.get("gate", []))

    # /list-alarms is the documented simulator source for components that
    # require maintenance. We do not invent a threshold.
    apply_alarm_snapshot(loaded.get("alarm", []), reason=f"sync:{reason}")

    try:
        zone_data = sim_request("GET", "/list-zones").json()
        if isinstance(zone_data, list):
            conn = db()
            for z in zone_data:
                name = str(z.get("name") or "").strip()
                if name:
                    conn.execute(
                        """INSERT INTO zones(name,co_level,danger,last_update)
                           VALUES(?,?,?,?)
                           ON CONFLICT(name) DO NOTHING""",
                        (name, z.get("gasCarbonMonoxideLevel"), str(z.get("risk") or ""), None)
                    )
            conn.commit()
            conn.close()
    except Exception as e:
        audit("system", "ZONE_SYNC_ERROR", "", str(e), "ERROR")

    prune_stale_components(loaded)
    infer_gate_roles()
    print_lane_discovery()

    counts = topology_counts()
    ready = level2_topology_ready()
    set_state("topology_loaded", "1" if ready else "0")

    print(
        "[LEVEL2 TOPOLOGY] "
        f"Park={counts['park']} Entry={counts['entry']} "
        f"Exit={counts['exit']} LeaveParking={counts['leave']} Gates={counts['gates']}"
    )
    if ready:
        print("[LEVEL2 TOPOLOGY] READY: exact Level 2 parking topology detected.")
    else:
        print("[LEVEL2 TOPOLOGY] NOT READY: waiting for the Level 2 map to finish loading.")

    audit(
        "system", "TOPOLOGY_SYNC", reason,
        ", ".join(f"{k}={len(v)}" for k, v in loaded.items())
        + f"; ready={ready}; counts={counts}"
    )


def infer_gate_roles():
    """
    The Level 2 /list-barriers API has no role field.
    Keep every barrier role UNKNOWN unless a physical lane mapping is learned
    from real simulator behaviour. Do not infer 'entry' or 'exit' from names.
    """
    conn = db()
    gates = [dict(r) for r in conn.execute(
        "SELECT name,zone FROM components WHERE kind='gate'"
    ).fetchall()]

    for gate in gates:
        conn.execute(
            """INSERT INTO gate_roles(name,role,zone,source)
               VALUES(?,?,?,?)
               ON CONFLICT(name) DO UPDATE SET
                 role=excluded.role,
                 zone=excluded.zone,
                 source=excluded.source""",
            (
                gate["name"],
                "unknown",
                gate.get("zone") or "",
                "api-does-not-expose-role"
            )
        )

    conn.commit()
    conn.close()


def component_row(name, kind=None):
    conn = db()
    if kind:
        row = conn.execute(
            "SELECT * FROM components WHERE name=? AND kind=?",
            (name, kind)
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT * FROM components WHERE name=? LIMIT 1",
            (name,)
        ).fetchone()
    conn.close()
    return dict(row) if row else None


def sensor_zone(sensor_name):
    """
    Return only the zoneParent actually exposed by the simulator API.
    EntrySpot rows currently return an empty zoneParent, so this returns ""
    rather than inventing an EntrySpot→Zone mapping.
    """
    row = component_row(sensor_name, "spot")
    return row.get("zone", "") if row else ""


def _number_suffix(value):
    m = re.search(r"(\d+)$", str(value or ""))
    return int(m.group(1)) if m else None


def remember_lane_mapping(sensor_name, role, gate, confidence="CONFIRMED", source="physical-crossing"):
    """Persist a sensor→barrier mapping learned from actual simulator behaviour."""
    if not sensor_name or not role or not gate:
        return
    conn = db()
    conn.execute(
        """INSERT INTO lane_map(sensor,role,gate,confidence,source,updated_at)
           VALUES(?,?,?,?,?,?)
           ON CONFLICT(sensor,role) DO UPDATE SET
             gate=excluded.gate,
             confidence=excluded.confidence,
             source=excluded.source,
             updated_at=excluded.updated_at""",
        (
            sensor_name,
            role,
            gate,
            confidence,
            source,
            simulator_now() or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
    )
    conn.commit()
    conn.close()
    audit(
        "system", "LANE_MAP_LEARNED", sensor_name,
        f"{role} sensor -> {gate}; confidence={confidence}; source={source}",
        "OK", simulator_now()
    )


def learned_gate_for_sensor(sensor_name, role):
    conn = db()
    row = conn.execute(
        """SELECT m.gate
           FROM lane_map m
           JOIN components c ON c.name=m.gate AND c.kind='gate'
           WHERE m.sensor=? AND m.role=?
             AND c.broken=0 AND c.under_maintenance=0
           LIMIT 1""",
        (sensor_name, role)
    ).fetchone()
    conn.close()
    return row["gate"] if row else None


def candidate_gates_for_sensor(sensor_name, role):
    """
    API-only barrier discovery.

    Priority:
    1) a sensor→gate mapping already CONFIRMED by real physical simulator events
    2) healthy barriers whose API zoneParent matches the sensor zoneParent
    3) other healthy barriers as deterministic discovery fallbacks

    No barrier is labelled entry/exit from its name or number.
    """
    learned = learned_gate_for_sensor(sensor_name, role)
    sensor_zone_name = sensor_zone(sensor_name)

    conn = db()
    rows = [dict(r) for r in conn.execute(
        """SELECT name,zone,state,broken,under_maintenance
           FROM components
           WHERE kind='gate'
             AND broken=0
             AND under_maintenance=0"""
    ).fetchall()]
    conn.close()

    ranked = []
    for row in rows:
        gate = row["name"]
        score = 0
        reasons = []

        if learned and gate == learned:
            score += 100000
            reasons.append("PHYSICALLY_CONFIRMED")

        if sensor_zone_name and row.get("zone") == sensor_zone_name:
            score += 1000
            reasons.append("API_SAME_ZONE")
        elif sensor_zone_name and not row.get("zone"):
            score += 100
            reasons.append("API_NO_ZONE_FALLBACK")

        # For an unzoned EntrySpot, prefer gates already Closed so discovery
        # does not disturb barriers the simulator intentionally loaded Open.
        if not sensor_zone_name:
            state = str(row.get("state") or "").lower()
            if state == "closed":
                score += 50
                reasons.append("API_STATE_CLOSED")
            elif state == "open":
                score -= 25
                reasons.append("API_STATE_OPEN_PRESERVE")

        ranked.append((score, gate, "+".join(reasons) or "API_HEALTHY_GATE"))

    ranked.sort(key=lambda x: (-x[0], x[1]))
    return ranked


def gate_for_sensor(sensor_name, role):
    """Return the best currently-known real barrier for this sensor."""
    learned = learned_gate_for_sensor(sensor_name, role)
    if learned:
        return learned

    candidates = candidate_gates_for_sensor(sensor_name, role)
    return candidates[0][1] if candidates else None


def find_active_entry_gate(plate):
    with state_lock:
        for gate, item in entry_active.items():
            if item.get("plate") == plate:
                return gate
    return None


def find_active_exit_gate(plate):
    with state_lock:
        for gate, item in exit_active.items():
            if item.get("plate") == plate:
                return gate
    return None


def print_lane_discovery():
    counts = topology_counts()

    conn = db()
    sensor_rows = [dict(r) for r in conn.execute(
        "SELECT name,zone,raw_json FROM components WHERE kind='spot'"
    ).fetchall()]
    gate_rows = [dict(r) for r in conn.execute(
        "SELECT name,zone,state,broken,under_maintenance FROM components WHERE kind='gate' ORDER BY name"
    ).fetchall()]
    conn.close()

    entries = []
    exits = []
    for row in sensor_rows:
        try:
            raw = json.loads(row.get("raw_json") or "{}")
        except Exception:
            raw = {}
        purpose = str(raw.get("purpose") or "")
        if purpose == "EntrySpot":
            entries.append((row["name"], row.get("zone") or ""))
        elif purpose == "ExitSpot":
            exits.append((row["name"], row.get("zone") or ""))

    print("\n====================================================")
    print(" PARKMIND LEVEL 2 — API-ONLY TOPOLOGY")
    print("====================================================")
    print(f" Parking bays : {counts['park']}")
    print(f" Entry sensors: {counts['entry']} -> {entries}")
    print(f" Exit sensors : {counts['exit']} -> {exits}")
    print(f" Barriers     : {counts['gates']}")
    print(" Barrier roles: NOT EXPOSED BY /list-barriers")
    print(" Barriers from API:")
    for g in gate_rows:
        print(
            f"   {g['name']}: zone={g.get('zone') or '-'} "
            f"state={g.get('state') or '?'} "
            f"broken={bool(g.get('broken'))} "
            f"maintenance={bool(g.get('under_maintenance'))}"
        )
    print("====================================================\n")


# ============================================================
# SIGNED WEBHOOK SECURITY
# ============================================================
def verify_webhook_signature(data):
    raw_sig = data.get("Signature")
    if raw_sig is None or str(raw_sig).strip().lower() in ("", "none", "null"):
        return None

    received = str(raw_sig).strip().lower()
    payload = {k: v for k, v in data.items() if k != "Signature"}
    values = []
    for key in sorted(payload.keys()):
        value = payload[key]
        if value is None:
            value = ""
        elif isinstance(value, float):
            value = repr(value)
        else:
            value = str(value)
        values.append(value)
    expected = hashlib.md5("|".join(values).encode("utf-8")).hexdigest()
    return expected == received


def save_event(data, signature_status):
    event_id = data.get("EventId") or None
    conn = db()
    try:
        conn.execute(
            """INSERT INTO events(
                event_id,sequence_id,event_class,server_time,signature_status,payload
            ) VALUES(?,?,?,?,?,?)""",
            (
                str(event_id) if event_id else None,
                data.get("SequenceId"),
                data.get("EventClass"),
                data.get("ServerDateTime"),
                signature_status,
                json.dumps(data)
            )
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()


def check_sequence(seq):
    global last_sequence_id
    if seq is None:
        return
    try:
        seq = int(seq)
    except Exception:
        return

    with last_sequence_lock:
        if last_sequence_id is not None and seq > last_sequence_id + 1:
            missing = seq - last_sequence_id - 1
            audit(
                "system", "SEQUENCE_GAP", "",
                f"Missing {missing} webhook(s): {last_sequence_id} -> {seq}",
                "RESYNC"
            )
            threading.Thread(
                target=lambda: safe_sync_topology("sequence-gap"),
                daemon=True
            ).start()
        last_sequence_id = max(last_sequence_id or 0, seq)


def safe_sync_topology(reason):
    try:
        sync_topology(reason)
    except Exception as e:
        audit("system", "TOPOLOGY_RESYNC_FAILED", reason, str(e), "ERROR")


# ============================================================
# COMPONENT COMMANDS + USAGE/RUNTIME
# ============================================================
def update_component_state(name, kind, state=None, broken=None, under=None,
                           cycle_add=0, sim_time=None):
    conn = db()
    row = conn.execute(
        "SELECT * FROM components WHERE name=? AND kind=?",
        (name, kind)
    ).fetchone()
    if not row:
        conn.close()
        return

    new_state = state if state is not None else row["state"]
    new_broken = int(broken) if broken is not None else int(row["broken"] or 0)
    new_under = int(under) if under is not None else int(row["under_maintenance"] or 0)
    cycles = int(row["cycles"] or 0) + int(cycle_add or 0)
    runtime = int(row["runtime_seconds"] or 0)
    on_since = row["on_since"]

    # Runtime uses simulator time only.
    if state is not None:
        if str(state).lower() == "on" and str(row["state"]).lower() != "on":
            on_since = sim_time or simulator_now() or on_since
        elif str(state).lower() == "off" and str(row["state"]).lower() == "on":
            stop = sim_time or simulator_now()
            runtime += sim_seconds_between(on_since, stop)
            on_since = None

    conn.execute(
        """UPDATE components SET
           state=?,broken=?,under_maintenance=?,cycles=?,runtime_seconds=?,
           on_since=?,last_event_time=?
           WHERE name=? AND kind=?""",
        (
            new_state, new_broken, new_under, cycles, runtime,
            on_since, sim_time or simulator_now() or row["last_event_time"],
            name, kind
        )
    )
    conn.commit()
    conn.close()


def component_action(kind, name, action, actor="system", sim_time=None):
    endpoints = {
        ("gate", "open"): f"/barrier-gates/{quote(name, safe='')}/open",
        ("gate", "close"): f"/barrier-gates/{quote(name, safe='')}/close",
        ("spot", "repair"): f"/parking-spots/{quote(name, safe='')}/repair",
        ("gate", "repair"): f"/barrier-gates/{quote(name, safe='')}/repair",
        ("light", "on"): f"/lights/{quote(name, safe='')}/on",
        ("light", "off"): f"/lights/{quote(name, safe='')}/off",
        ("fan", "on"): f"/exhaust-fans/{quote(name, safe='')}/on",
        ("fan", "off"): f"/exhaust-fans/{quote(name, safe='')}/off",
        ("fan", "repair"): f"/exhaust-fans/{quote(name, safe='')}/repair",
    }
    path = endpoints.get((kind, action))
    if not path:
        raise ValueError(f"Unsupported component action: {kind}/{action}")

    row = component_row(name, kind)
    if not row:
        raise ValueError(f"Unknown simulator component: {kind}/{name}")

    if action != "repair":
        if int(row.get("broken") or 0):
            raise RuntimeError(f"{name} is broken and cannot be operated")
        if int(row.get("under_maintenance") or 0):
            raise RuntimeError(f"{name} is under maintenance and cannot be operated")

    try:
        sim_request("POST", path)
        if action in ("on", "off"):
            update_component_state(
                name, kind, action.capitalize(),
                cycle_add=1 if action == "on" else 0,
                sim_time=sim_time
            )
        elif kind == "gate" and action in ("open", "close"):
            # gate cycles are primarily confirmed by gate_action webhooks
            pass
        elif action == "repair":
            update_component_state(name, kind, under=True, sim_time=sim_time)
        audit(actor, f"{kind.upper()}_{action.upper()}", name, "", "OK", sim_time)
        return True
    except Exception as e:
        audit(actor, f"{kind.upper()}_{action.upper()}", name, str(e), "ERROR", sim_time)
        raise


def send_car(plate, destination):
    plate_path = quote(str(plate).replace(" ", ""), safe="")
    dest = quote(str(destination), safe="")
    sim_request("POST", f"/car/{plate_path}/goto/{dest}")
    audit("system", "CAR_GOTO", plate, str(destination), "OK", simulator_now())


def charge_car(plate, parking_cost, charging_cost):
    plate_path = quote(str(plate).replace(" ", ""), safe="")
    sim_request(
        "POST",
        f"/car/{plate_path}/charge",
        params={
            "parkingCost": parking_cost,
            "chargingCost": charging_cost
        }
    )
    audit(
        "system", "CHARGE_REQUEST", plate,
        f"parking={parking_cost}, charging={charging_cost}",
        "OK", simulator_now()
    )


# ============================================================
# SMART PARKING / RESERVATIONS
# ============================================================
def is_compatible(raw, car_type):
    target = str(raw.get("parkingForCarType") or "Any").lower()
    car = str(car_type or "Normal").lower()
    if target in ("", "any"):
        return True
    if "electric" in car and "electric" in target:
        return True
    if "accessible" in car and "accessible" in target:
        return True
    return target == car


def cleanup_stale_reservations(sim_time=None):
    """
    Level-1 reservation safety restored for Level 2.

    Reservations are software locks, not physical occupancy. If a route failed
    or the simulator was restarted, an old reservation must not make a truly
    free simulator bay look unavailable forever.
    """
    now_value = sim_time or simulator_now()
    now_dt = parse_sim_time(now_value)

    conn = db()
    rows = [dict(r) for r in conn.execute(
        """SELECT r.spot,r.plate,r.reserved_at,
                  c.name AS component_name,c.state,c.broken,c.under_maintenance,c.raw_json
           FROM reservations r
           LEFT JOIN components c ON c.name=r.spot AND c.kind='spot'"""
    ).fetchall()]

    released = []
    for row in rows:
        release_reason = None

        # Reservation references a spot that no longer belongs to this loaded level.
        if not row.get("component_name"):
            release_reason = "spot no longer exists in current simulator topology"
        else:
            try:
                raw = json.loads(row.get("raw_json") or "{}")
            except Exception:
                raw = {}

            if str(raw.get("purpose") or "") != "Park":
                release_reason = "reserved target is not a parking bay"
            elif int(row.get("broken") or 0):
                release_reason = "spot became broken"
            elif int(row.get("under_maintenance") or 0):
                release_reason = "spot entered maintenance"

        # Age is based on simulator timestamps, not the laptop clock.
        if release_reason is None and now_dt:
            reserved_dt = parse_sim_time(row.get("reserved_at"))
            if reserved_dt:
                age = (now_dt - reserved_dt).total_seconds()
                if age < -5:
                    # Simulator was reloaded/reset and its clock moved backwards.
                    release_reason = "reservation belongs to an older simulator session"
                elif age > RESERVATION_TTL_SECONDS:
                    release_reason = f"reservation expired after {int(age)} simulator seconds"

        if release_reason:
            conn.execute("DELETE FROM reservations WHERE spot=?", (row["spot"],))
            released.append((row["spot"], row["plate"], release_reason))

    conn.commit()
    conn.close()

    for spot, plate, why in released:
        audit(
            "system", "STALE_RESERVATION_RELEASED", plate,
            f"{spot}: {why}", "OK", now_value
        )

    if released:
        print(f"[RESERVATION] Released {len(released)} stale reservation(s).")

    return len(released)


def refresh_parking_spots_from_simulator(reason):
    """
    One exceptional reconciliation call.

    This is NOT polling. It is used only when PARKMIND thinks a zone has no
    assignable bay, so we verify the physical simulator state before rejecting
    the arriving car.
    """
    data = sim_request("GET", "/list-parking-spots").json()
    if not isinstance(data, list):
        raise RuntimeError("Simulator /list-parking-spots did not return a list")

    current_names = set()
    for item in data:
        name = str(item.get("name") or "").strip()
        if name:
            current_names.add(name)
        upsert_component("spot", item, simulator_now())

    # Remove spot rows left over from another loaded level.
    conn = db()
    if current_names:
        placeholders = ",".join("?" for _ in current_names)
        conn.execute(
            f"DELETE FROM components WHERE kind='spot' AND name NOT IN ({placeholders})",
            list(current_names)
        )
    conn.commit()
    conn.close()

    audit(
        "system", "SPOT_STATE_RESYNC", reason,
        f"Simulator returned {len(data)} spot/sensor objects",
        "OK", simulator_now()
    )


def _spot_selection_snapshot(preferred_zone, car_type):
    """Diagnostic only: explains why a car did or did not get a bay."""
    conn = db()
    rows = [dict(r) for r in conn.execute(
        "SELECT * FROM components WHERE kind='spot'"
    ).fetchall()]
    reservations = {
        r["spot"] for r in conn.execute("SELECT spot FROM reservations").fetchall()
    }
    conn.close()

    stats = {
        "park_total": 0,
        "zone_total": 0,
        "zone_free": 0,
        "zone_reserved": 0,
        "zone_broken": 0,
        "zone_maintenance": 0,
        "zone_compatible_free": 0,
    }

    for row in rows:
        try:
            raw = json.loads(row.get("raw_json") or "{}")
        except Exception:
            raw = {}

        if str(raw.get("purpose") or "") != "Park":
            continue
        stats["park_total"] += 1

        if preferred_zone and row.get("zone") != preferred_zone:
            continue

        stats["zone_total"] += 1
        if int(row.get("broken") or 0):
            stats["zone_broken"] += 1
        if int(row.get("under_maintenance") or 0):
            stats["zone_maintenance"] += 1
        if row["name"] in reservations:
            stats["zone_reserved"] += 1
        if row.get("state") == "Free":
            stats["zone_free"] += 1
            if (
                not int(row.get("broken") or 0)
                and not int(row.get("under_maintenance") or 0)
                and row["name"] not in reservations
                and is_compatible(raw, car_type)
            ):
                stats["zone_compatible_free"] += 1

    return stats


def choose_spot(car_type, preferred_zone=None, allow_resync=True):
    """
    Dynamic anti-pile selection:
      1) collect real free/healthy/compatible Park bays
      2) group them by API zoneParent
      3) choose the least-loaded compatible zone FIRST
      4) then choose the best bay inside that zone

    Occupied + reserved/in-flight bays count toward zone pressure.
    """
    cleanup_stale_reservations()

    conn = db()
    free_rows = [dict(r) for r in conn.execute(
        """SELECT c.*
           FROM components c
           WHERE c.kind='spot'
             AND c.broken=0
             AND c.under_maintenance=0
             AND c.state='Free'
             AND NOT EXISTS(
               SELECT 1 FROM reservations r WHERE r.spot=c.name
             )"""
    ).fetchall()]

    all_spots = [dict(r) for r in conn.execute(
        "SELECT * FROM components WHERE kind='spot'"
    ).fetchall()]

    reserved_names = {
        r["spot"]
        for r in conn.execute("SELECT spot FROM reservations").fetchall()
    }
    conn.close()

    zone_counts = {}
    for row in all_spots:
        try:
            raw = json.loads(row.get("raw_json") or "{}")
        except Exception:
            raw = {}

        if str(raw.get("purpose") or "") != "Park":
            continue

        zone = str(row.get("zone") or "").strip()
        if not zone:
            continue

        z = zone_counts.setdefault(zone, {"total": 0, "busy": 0})
        z["total"] += 1

        if str(row.get("state") or "") == "Occupied":
            z["busy"] += 1
        elif row["name"] in reserved_names:
            z["busy"] += 1

    candidates_by_zone = defaultdict(list)

    for row in free_rows:
        try:
            raw = json.loads(row.get("raw_json") or "{}")
        except Exception:
            raw = {}

        if str(raw.get("purpose") or "") != "Park":
            continue

        zone = str(row.get("zone") or "").strip()
        if not zone:
            continue
        if preferred_zone and zone != preferred_zone:
            continue
        if not is_compatible(raw, car_type):
            continue

        candidates_by_zone[zone].append((row, raw))

    if not candidates_by_zone:
        stats = _spot_selection_snapshot(preferred_zone, car_type)
        print(
            f"[SPOT SELECT] No compatible candidate for "
            f"car_type={car_type} preferred_zone={preferred_zone}. stats={stats}"
        )

        if allow_resync:
            try:
                refresh_parking_spots_from_simulator(
                    f"no-candidate:{car_type}:{preferred_zone}"
                )
                cleanup_stale_reservations()
                return choose_spot(
                    car_type,
                    preferred_zone=preferred_zone,
                    allow_resync=False
                )
            except Exception as e:
                audit(
                    "system", "SPOT_STATE_RESYNC_FAILED",
                    preferred_zone or "", str(e), "ERROR", simulator_now()
                )

        return None, None

    # Choose the least-loaded compatible zone.
    zone_rank = []
    for zone, zone_candidates in candidates_by_zone.items():
        counts = zone_counts.get(
            zone, {"total": len(zone_candidates), "busy": 0}
        )
        total = max(1, int(counts["total"]))
        busy = int(counts["busy"])
        load = busy / total

        zone_rank.append(
            (load, busy, -len(zone_candidates), zone)
        )

    zone_rank.sort()
    chosen_zone = zone_rank[0][3]
    chosen_load = zone_rank[0][0]

    # Choose the best compatible bay only inside the selected zone.
    zone_candidates = candidates_by_zone[chosen_zone]
    max_cycles = max(
        [int(row.get("cycles") or 0) for row, _ in zone_candidates] + [1]
    )

    ranked = []
    for row, raw in zone_candidates:
        cycles = int(row.get("cycles") or 0)
        condition = 1.0 - (cycles / max_cycles)

        target = str(raw.get("parkingForCarType") or "Any").lower()
        car = str(car_type or "Normal").lower()

        type_rank = 1
        if "electric" in car and target == "electric":
            type_rank = 0
        elif "accessible" in car and target == "accessible":
            type_rank = 0
        elif target not in ("", "any"):
            type_rank = 2

        ranked.append(
            (
                type_rank,
                -condition,
                cycles,
                _number_suffix(row["name"]),
                row["name"]
            )
        )

    ranked.sort()
    selected_spot = ranked[0][4]

    load_text = ", ".join(
        f"{zone}={zone_counts.get(zone, {}).get('busy', 0)}/"
        f"{zone_counts.get(zone, {}).get('total', 0)}"
        for zone in sorted(candidates_by_zone)
    )

    print(
        f"[ZONE BALANCE] car={car_type} -> zone={chosen_zone}; "
        f"assigned={selected_spot}; loads=[{load_text}]"
    )
    audit(
        "system", "ZONE_BALANCED_ASSIGNMENT", selected_spot,
        (
            f"car_type={car_type}; chosen_zone={chosen_zone}; "
            f"zone_load={chosen_load:.3f}; loads=[{load_text}]"
        ),
        "OK", simulator_now()
    )

    return selected_spot, chosen_zone


def reserve_spot(spot, plate, sim_time):
    conn = db()
    conn.execute(
        "INSERT OR REPLACE INTO reservations(spot,plate,reserved_at) VALUES(?,?,?)",
        (spot, plate, sim_time)
    )
    conn.commit()
    conn.close()


def release_reservation(spot=None, plate=None):
    conn = db()
    if spot:
        conn.execute("DELETE FROM reservations WHERE spot=?", (spot,))
    elif plate:
        conn.execute("DELETE FROM reservations WHERE plate=?", (plate,))
    conn.commit()
    conn.close()


# ============================================================
# MULTI-ENTRY CONTROL — LEVEL 1 FLOW, LEVEL 2 TOPOLOGY
# ============================================================
def _entry_item_candidates(entry_spot):
    return [gate for _, gate, _ in candidate_gates_for_sensor(entry_spot, "entry")]


def _requeue_entry_on_next_candidate(item, failed_gate, reason):
    candidates = list(item.get("gate_candidates") or [])
    tried = set(item.get("tried_gates") or [])
    tried.add(failed_gate)

    next_gate = next((g for g in candidates if g not in tried), None)
    if not next_gate:
        # The car never entered, so its software bay reservation must be released.
        release_reservation(plate=item["plate"])
        upsert_car(
            item["plate"],
            assigned_spot=None,
            status="ENTRY_HOLD",
            decision=f"No confirmed entrance barrier after trying {sorted(tried)}"
        )
        upsert_alert(
            f"ENTRY_DISCOVERY:{item['plate']}",
            "CRITICAL", "ENTRY LANE DISCOVERY FAILED",
            f"Could not confirm a working barrier for {item.get('entry_spot')}. "
            f"Tried {sorted(tried)}. Last reason: {reason}",
            simulator_now(),
            item["plate"],
            item.get("entry_zone") or ""
        )
        return

    item["tried_gates"] = list(tried)
    item["gate"] = next_gate
    # A new physical gate trial must be allowed to re-send the route command.
    item["sent"] = False
    item["route_sent_at"] = None
    audit(
        "system", "ENTRY_GATE_DISCOVERY_RETRY", item["plate"],
        f"{failed_gate} -> {next_gate}; reason={reason}",
        "RETRY", simulator_now()
    )
    with state_lock:
        entry_queues[next_gate].insert(0, item)
    process_entry_gate(next_gate)


def delayed_entry_route_release(gate, plate, spot):
    """
    Hold the car until the full API-derived route is ready.

    Required before goto/<spot>:
      - no global gate lockdown
      - selected entry barrier == Open
      - every healthy barrier attached to target bay's zoneParent == Open
    """
    target_zone = target_zone_for_spot(spot)
    prepare_zone_access_for_car(plate, spot)

    while True:
        time.sleep(0.25)

        with state_lock:
            item = entry_active.get(gate)
            if not item or item.get("plate") != plate or item.get("sent"):
                return

        if not gate_traffic_allowed(refresh_live=True):
            release_reservation(plate=plate)
            release_zone_access_for_car(plate, target_zone)
            with state_lock:
                entry_active.pop(gate, None)
            upsert_car(
                plate,
                assigned_spot=None,
                status="GATE_LOCKDOWN_ENTRY",
                decision="Global barrier lockdown before entry release"
            )
            return

        if not gate_physically_open(gate):
            try:
                component_action(
                    "gate", gate, "open", "system", simulator_now()
                )
            except Exception:
                pass
            continue

        if not zone_access_is_open(target_zone):
            prepare_zone_access_for_car(plate, spot)
            continue

        try:
            send_car(plate, spot)
            with state_lock:
                current = entry_active.get(gate)
                if current and current.get("plate") == plate:
                    current["sent"] = True
                    current["route_sent_at"] = time.time()

            upsert_car(plate, status="TO_SPOT")
            audit(
                "system", "ENTRY_ROUTE_RELEASED", plate,
                (
                    f"entry_gate={gate}; target_zone={target_zone}; "
                    f"destination={spot}; full route confirmed open"
                ),
                "OK", simulator_now()
            )
        except Exception as e:
            audit(
                "system", "ENTRY_ROUTE_RELEASE_FAILED", plate,
                str(e), "ERROR", simulator_now()
            )
        return


def entry_lane_discovery_watch(gate, plate):
    """
    Entry gate lifecycle:

      request Open
        -> wait for API state == Open
        -> send car to assigned bay
        -> KEEP THIS GATE OPEN
        -> wait as long as necessary for EntrySpot CarOut
        -> only CarOut is allowed to release/restore the gate

    A slow car is never punished by closing the door underneath it.
    Gate discovery fallback is used only when the car was NEVER released.
    """
    started = time.time()

    while True:
        time.sleep(0.5)

        with state_lock:
            current = entry_active.get(gate)
            if not current or current.get("plate") != plate:
                # Real EntrySpot CarOut removed it. Crossing is complete.
                return
            sent = bool(current.get("sent"))

        if sent:
            # The car has been authorized to cross. Keep the physical gate open
            # until EntrySpot CarOut removes entry_active[gate].
            row = component_row(gate, "gate") or {}
            if int(row.get("broken") or 0) or int(row.get("under_maintenance") or 0):
                upsert_alert(
                    f"ENTRY_GATE_FAILED_DURING_CROSSING:{plate}",
                    "CRITICAL",
                    "ENTRY GATE FAILED DURING CROSSING",
                    f"{gate} became unavailable while {plate} was crossing.",
                    simulator_now(),
                    plate,
                    component=gate
                )
                continue

            live_state = refresh_live_gate_state(gate).strip().lower()
            if live_state not in ("open", "opening"):
                try:
                    component_action("gate", gate, "open", "system", simulator_now())
                    audit(
                        "system",
                        "ENTRY_GATE_HOLD_OPEN",
                        plate,
                        f"Re-opened {gate}; waiting for real EntrySpot CarOut.",
                        "OK",
                        simulator_now()
                    )
                except Exception as e:
                    audit(
                        "system",
                        "ENTRY_GATE_HOLD_OPEN",
                        plate,
                        str(e),
                        "ERROR",
                        simulator_now()
                    )
            continue

        # If the entry gate is already Open, any remaining delay is target-zone
        # access. Do not switch to another entrance barrier and create a pile.
        if gate_physically_open(gate):
            continue

        # The candidate entry gate itself never became usable.
        if time.time() - started < LANE_DISCOVERY_TIMEOUT_SEC:
            continue

        with state_lock:
            current = entry_active.get(gate)
            if not current or current.get("plate") != plate:
                return
            if current.get("sent"):
                continue
            item = dict(current)
            entry_active.pop(gate, None)

        try:
            auto_restore_gate_api_baseline(gate, "system", simulator_now())
        except Exception:
            pass

        _requeue_entry_on_next_candidate(
            item,
            gate,
            f"{gate} never produced a confirmed Open release within "
            f"{LANE_DISCOVERY_TIMEOUT_SEC:.1f}s"
        )
        return


def refresh_all_gate_health_from_api():
    """
    Refresh ALL barrier health from the real /list-barriers endpoint.

    No gate names or roles are assumed. Every barrier returned by the API
    participates in the global safety interlock.
    """
    try:
        items = sim_request("GET", "/list-barriers").json()
        if not isinstance(items, list):
            return False

        now_value = simulator_now()
        for item in items:
            upsert_component("gate", item, now_value)
        return True
    except Exception as e:
        audit(
            "system",
            "GATE_HEALTH_REFRESH_FAILED",
            "",
            str(e),
            "USING_LAST_KNOWN_STATE",
            simulator_now()
        )
        return False


def gate_lockdown_status(refresh_live=False):
    """
    Return (locked, problem_gates).

    GLOBAL rule requested for Level 2:
      if ANY simulator barrier is broken OR under maintenance,
      PARKMIND admits nobody and releases nobody.

    This is completely dynamic: every gate comes from /list-barriers.
    """
    if refresh_live:
        refresh_all_gate_health_from_api()

    conn = db()
    rows = [dict(r) for r in conn.execute(
        """SELECT name,zone,state,broken,under_maintenance
           FROM components
           WHERE kind='gate'
             AND (broken=1 OR under_maintenance=1)
           ORDER BY name"""
    ).fetchall()]
    conn.close()

    return bool(rows), rows


def gate_lockdown_reason(problem_gates):
    parts = []
    for row in problem_gates:
        flags = []
        if int(row.get("broken") or 0):
            flags.append("BROKEN")
        if int(row.get("under_maintenance") or 0):
            flags.append("UNDER_MAINTENANCE")
        parts.append(
            f"{row.get('name')}[{'+'.join(flags) or 'UNAVAILABLE'}]"
        )
    return ", ".join(parts)


def mark_gate_lockdown(problem_gates, sim_time=None):
    reason = gate_lockdown_reason(problem_gates)
    upsert_alert(
        "GLOBAL_GATE_LOCKDOWN",
        "CRITICAL",
        "CAR PARK GATE LOCKDOWN",
        (
            f"Entry and exit are blocked because barrier(s) are unavailable: "
            f"{reason}. Maintenance must repair them and PARKMIND must receive "
            f"the real component_fixed/API healthy state before traffic resumes."
        ),
        sim_time or simulator_now()
    )
    set_state("gate_lockdown", "1")
    audit(
        "system",
        "GLOBAL_GATE_LOCKDOWN",
        "",
        reason,
        "ENTRY_AND_EXIT_BLOCKED",
        sim_time or simulator_now()
    )


def hold_unreleased_gate_traffic(problem_gates, sim_time=None):
    """
    Freeze all NOT-YET-RELEASED gate traffic.

    Commands that were already sent to a moving car cannot be recalled from the
    simulator. We do not issue any NEW entry/exit movement after lockdown.
    """
    held_entries = []

    with state_lock:
        # Cars waiting in per-gate entry queues have not been released yet.
        for gate, queue in list(entry_queues.items()):
            while queue:
                held_entries.append(dict(queue.pop(0)))

        # Active entry item is safe to hold only if no goto command was sent yet.
        for gate, item in list(entry_active.items()):
            if not item.get("sent"):
                held_entries.append(dict(item))
                entry_active.pop(gate, None)

        # Paid exit releases not yet sent are cancelled and will resume after fix.
        for gate, item in list(exit_active.items()):
            if not item.get("sent"):
                plate = item.get("plate")
                if plate:
                    upsert_car(
                        plate,
                        status="PAID_GATE_LOCKDOWN",
                        decision="Global barrier lockdown: waiting for maintenance repair"
                    )
                exit_active.pop(gate, None)

        # Remove software release queues; PAID state remains stored in cars table.
        for gate in list(paid_exit_queues.keys()):
            paid_exit_queues[gate].clear()

    # Entry holds should not keep parking bays reserved for an indefinite outage.
    seen = set()
    for item in held_entries:
        plate = item.get("plate")
        if not plate or plate in seen:
            continue
        seen.add(plate)
        release_reservation(plate=plate)
        upsert_car(
            plate,
            assigned_spot=None,
            status="GATE_LOCKDOWN_ENTRY",
            decision=(
                "Global barrier lockdown: no admission until all gates are "
                "healthy after maintenance"
            )
        )

    mark_gate_lockdown(problem_gates, sim_time)


def gate_traffic_allowed(refresh_live=False, sim_time=None):
    locked, problems = gate_lockdown_status(refresh_live=refresh_live)
    if locked:
        mark_gate_lockdown(problems, sim_time)
        return False
    return True


def resume_after_gate_lockdown(sim_time=None):
    """
    Resume only when EVERY API/DB barrier is healthy.

    A signed component_fixed event from the repair flow updates the repaired
    gate. If another gate is still broken/under maintenance, lockdown remains.
    """
    locked, problems = gate_lockdown_status(refresh_live=False)
    if locked:
        mark_gate_lockdown(problems, sim_time)
        return False

    set_state("gate_lockdown", "0")
    resolve_alert("GLOBAL_GATE_LOCKDOWN", sim_time or simulator_now())
    audit(
        "system",
        "GLOBAL_GATE_LOCKDOWN_CLEARED",
        "",
        "All API-reported barriers are healthy.",
        "TRAFFIC_RESUMED",
        sim_time or simulator_now()
    )

    # Re-admit cars that were physically waiting at EntrySpot.
    conn = db()
    entry_waiters = [dict(r) for r in conn.execute(
        """SELECT plate,car_type,planned_minutes,entry_spot,entry_time
           FROM cars
           WHERE status='GATE_LOCKDOWN_ENTRY'
             AND departure_time IS NULL
           ORDER BY entry_time ASC"""
    ).fetchall()]

    paid_waiters = [dict(r) for r in conn.execute(
        """SELECT plate
           FROM cars
           WHERE payment_status='PAID'
             AND exit_spot IS NOT NULL
             AND departure_time IS NULL
             AND status IN('PAID_GATE_LOCKDOWN','PAID_WAITING_GATE','PAID')
           ORDER BY exit_arrival_time ASC"""
    ).fetchall()]
    conn.close()

    for car in entry_waiters:
        try:
            enqueue_arrival(
                car["plate"],
                car.get("car_type") or "Normal",
                int(car.get("planned_minutes") or 0),
                car.get("entry_spot") or "",
                car.get("entry_time") or sim_time or simulator_now()
            )
        except Exception as e:
            audit(
                "system",
                "ENTRY_RESUME_AFTER_GATE_FIX_FAILED",
                car["plate"],
                str(e),
                "ERROR",
                sim_time or simulator_now()
            )

    # Parked vehicles whose planned exit matured during lockdown remain in
    # exit_request_queue; restart that dispatcher now.
    dispatch_exit_requests()

    # Already-paid cars physically waiting at ExitSpots may now release.
    for car in paid_waiters:
        try:
            queue_paid_vehicle_for_release(car["plate"])
        except Exception as e:
            audit(
                "system",
                "PAID_EXIT_RESUME_AFTER_GATE_FIX_FAILED",
                car["plate"],
                str(e),
                "ERROR",
                sim_time or simulator_now()
            )

    return True


def target_zone_for_spot(spot_name):
    row = component_row(spot_name, "spot") or {}
    return str(row.get("zone") or "").strip()


def healthy_gates_in_zone(zone_name):
    """
    Return healthy barriers whose API zoneParent matches the target bay's
    API zoneParent. No entry/exit role is invented.
    """
    if not zone_name:
        return []

    conn = db()
    rows = [dict(r) for r in conn.execute(
        """SELECT name,state,broken,under_maintenance
           FROM components
           WHERE kind='gate' AND zone=?
           ORDER BY name""",
        (zone_name,)
    ).fetchall()]
    conn.close()

    return [
        row for row in rows
        if not int(row.get("broken") or 0)
        and not int(row.get("under_maintenance") or 0)
    ]


def prepare_zone_access_for_car(plate, spot_name):
    """
    Before routing to a bay, open healthy barriers attached by the API to that
    bay's target zone. They stay open until a real Park CarIn confirms arrival.
    """
    zone_name = target_zone_for_spot(spot_name)
    if not zone_name:
        return ""

    with state_lock:
        zone_access_users[zone_name].add(plate)

    for row in healthy_gates_in_zone(zone_name):
        gate = row["name"]
        state = str(row.get("state") or "").strip().lower()
        if state not in ("open", "opening"):
            try:
                component_action("gate", gate, "open", "system", simulator_now())
                audit(
                    "system", "ZONE_ACCESS_OPEN_REQUEST", plate,
                    f"target_zone={zone_name}; barrier={gate}",
                    "OK", simulator_now()
                )
            except Exception as e:
                audit(
                    "system", "ZONE_ACCESS_OPEN_REQUEST", plate,
                    f"target_zone={zone_name}; barrier={gate}; {e}",
                    "ERROR", simulator_now()
                )

    return zone_name


def zone_access_is_open(zone_name):
    """
    A target zone is ready only when every healthy barrier whose API zoneParent
    matches that zone is confirmed Open by /list-barriers.
    """
    if not zone_name:
        return True

    try:
        data = sim_request("GET", "/list-barriers").json()
        if not isinstance(data, list):
            return False

        matched = []
        now_value = simulator_now()

        for item in data:
            upsert_component("gate", item, now_value)

            if str(item.get("zoneParent") or "").strip() != zone_name:
                continue
            if bool(item.get("broken", False)):
                return False
            if bool(item.get("isUnderMaintenance", False)):
                return False
            matched.append(item)

        if not matched:
            return True

        return all(
            str(item.get("state") or "").strip().lower() == "open"
            for item in matched
        )

    except Exception as e:
        audit(
            "system", "ZONE_ACCESS_STATE_READ_FAILED", zone_name,
            str(e), "NOT_READY", simulator_now()
        )
        return False


def release_zone_access_for_car(plate, zone_name=None):
    """
    Restore target-zone barriers only after a real Park CarIn and only when no
    other in-flight car still needs that same zone.
    """
    if not zone_name:
        car = get_car(plate) or {}
        spot = car.get("actual_spot") or car.get("assigned_spot")
        zone_name = target_zone_for_spot(spot) if spot else ""

    if not zone_name:
        return

    with state_lock:
        users = zone_access_users.get(zone_name)
        if users:
            users.discard(plate)
        still_needed = bool(users)
        if not still_needed:
            zone_access_users.pop(zone_name, None)

    if still_needed:
        return

    for row in healthy_gates_in_zone(zone_name):
        try:
            auto_restore_gate_api_baseline(
                row["name"], "system", simulator_now()
            )
        except Exception as e:
            audit(
                "system", "ZONE_ACCESS_RESTORE_FAILED", row["name"],
                str(e), "ERROR", simulator_now()
            )


def delayed_restore_entry_gate_if_idle(gate):
    """
    Short idle grace avoids close/open flapping when simulator cars arrive
    back-to-back at the same EntrySpot.
    """
    time.sleep(ENTRY_IDLE_RESTORE_GRACE_SEC)

    with state_lock:
        if gate in entry_active:
            return
        if entry_queues.get(gate):
            return

    try:
        auto_restore_gate_api_baseline(
            gate, "system", simulator_now()
        )
    except Exception as e:
        audit(
            "system", "ENTRY_IDLE_RESTORE_FAILED", gate,
            str(e), "ERROR", simulator_now()
        )


def refresh_live_gate_state(gate):
    """
    Read the CURRENT barrier state directly from /list-barriers.

    This is intentionally used at the moment of vehicle release so PARKMIND
    never relies on a stale SQLite state or on a guessed timing delay.
    """
    try:
        data = sim_request("GET", "/list-barriers").json()
        if not isinstance(data, list):
            return ""

        for item in data:
            if str(item.get("name") or "") != str(gate):
                continue

            # Keep our DB snapshot aligned with the API response.
            upsert_component("gate", item)
            return str(item.get("state") or "")

    except Exception as e:
        audit(
            "system",
            "GATE_LIVE_STATE_READ_FAILED",
            gate,
            str(e),
            "NO_RELEASE",
            simulator_now()
        )

    return ""


def gate_physically_open(gate):
    """
    Release interlock: only literal API/webhook state 'Open' is accepted.
    'Opening', 'Closed', 'Closing', unknown or API failure are NOT enough.
    """
    live_state = refresh_live_gate_state(gate)
    return live_state.strip().lower() == "open"


def process_entry_gate(gate):
    with state_lock:
        if gate in entry_active or not entry_queues[gate]:
            return
        item = entry_queues[gate].pop(0)
        item["gate"] = gate
        item.setdefault("tried_gates", [])
        entry_active[gate] = item

    plate = item["plate"]
    spot = item["spot"]

    # Dynamic target-zone access: Park bay zoneParent -> barrier zoneParent.
    target_zone = prepare_zone_access_for_car(plate, spot)

    # Fail closed if any barrier anywhere in the car park is unavailable.
    locked, gate_problems = gate_lockdown_status(refresh_live=True)
    if locked:
        with state_lock:
            entry_active.pop(gate, None)
        release_reservation(plate=plate)
        release_zone_access_for_car(plate, target_zone)
        upsert_car(
            plate,
            assigned_spot=None,
            status="GATE_LOCKDOWN_ENTRY",
            decision="Global barrier lockdown before entry release"
        )
        hold_unreleased_gate_traffic(gate_problems, simulator_now())
        return

    try:
        gate_row = component_row(gate, "gate") or {}
        if int(gate_row.get("broken") or 0) or int(gate_row.get("under_maintenance") or 0):
            raise RuntimeError(f"{gate} is broken or under maintenance")

        # Release only when the entry gate AND target-zone barriers are open.
        if gate_physically_open(gate) and zone_access_is_open(target_zone):
            send_car(plate, spot)
            with state_lock:
                if gate in entry_active and entry_active[gate].get("plate") == plate:
                    entry_active[gate]["sent"] = True
                    entry_active[gate]["route_sent_at"] = time.time()
            upsert_car(plate, status="TO_SPOT")
        else:
            component_action("gate", gate, "open", "system", simulator_now())

        threading.Thread(
            target=delayed_entry_route_release,
            args=(gate, plate, spot),
            daemon=True
        ).start()
        threading.Thread(
            target=entry_lane_discovery_watch,
            args=(gate, plate),
            daemon=True
        ).start()

    except Exception as e:
        with state_lock:
            entry_active.pop(gate, None)
        _requeue_entry_on_next_candidate(item, gate, str(e))


def enqueue_arrival(plate, car_type, planned, entry_spot, sim_time):
    # If PARKMIND started before the user clicked "Load Level 2", the first
    # signed entry event performs the one required topology sync.
    if not ensure_level2_topology_ready(f"entry:{entry_spot}"):
        upsert_alert(
            f"TOPOLOGY_NOT_READY:{entry_spot}",
            "CRITICAL", "LEVEL 2 TOPOLOGY NOT READY",
            "The simulator has not exposed the required Level 2 component topology yet.",
            sim_time, plate
        )
        return

    entry_api_zone = sensor_zone(entry_spot)

    # GLOBAL BARRIER INTERLOCK:
    # if ANY gate is broken/under maintenance, this car remains at EntrySpot.
    locked, gate_problems = gate_lockdown_status(refresh_live=True)
    if locked:
        upsert_car(
            plate,
            car_type=car_type,
            planned_minutes=planned,
            entry_time=sim_time,
            entry_spot=entry_spot,
            entry_zone=entry_api_zone,
            assigned_spot=None,
            status="GATE_LOCKDOWN_ENTRY",
            decision=(
                "Admission blocked until all simulator barriers are healthy: "
                + gate_lockdown_reason(gate_problems)
            )
        )
        hold_unreleased_gate_traffic(gate_problems, sim_time)
        return

    # Record the trustworthy simulator entry immediately, even if lane discovery
    # later needs to try more than one barrier.
    upsert_car(
        plate,
        car_type=car_type,
        planned_minutes=planned,
        entry_time=sim_time,
        entry_spot=entry_spot,
        entry_zone=entry_api_zone,
        status="WAITING_ENTRY"
    )

    # EntrySpot.zoneParent is not exposed by the Level 2 API.
    # Therefore choose across ALL real Park bays and let API zoneParent +
    # current occupancy/reservations balance cars across ZONE1/2/3.
    spot, spot_zone = choose_spot(car_type, preferred_zone=None)
    if not spot:
        stats = _spot_selection_snapshot(None, car_type)
        reason = (
            f"After a live simulator resync, no compatible free healthy bay "
            f"is available anywhere in the API-reported parking zones. stats={stats}"
        )
        upsert_car(
            plate,
            status="NO_SAFE_SPACE",
            decision=reason
        )
        upsert_alert(
            f"NO_SPACE:{plate}", "HIGH", "NO SAFE PARKING SPACE",
            reason,
            sim_time, plate, entry_api_zone
        )

        # This is now an intentional rejection only after the physical API was
        # re-checked. It should never happen while the simulator still has a
        # compatible free bay in this zone.
        print(f"[ENTRY REJECT] {plate}: {reason}")
        try:
            send_car(plate, "leavepark")
        except Exception as e:
            audit(
                "system", "ENTRY_REJECT_LEAVEPARK_FAILED",
                plate, str(e), "ERROR", sim_time
            )
        return

    # The assigned parking bay's zoneParent is real API data.
    upsert_car(
        plate,
        entry_zone=spot_zone,
        decision=f"Assigned {spot} in API zone {spot_zone}"
    )

    candidates = _entry_item_candidates(entry_spot)
    if not candidates:
        upsert_car(
            plate, assigned_spot=None, status="ENTRY_HOLD",
            decision="No healthy entrance barrier candidate"
        )
        upsert_alert(
            f"ENTRY_MAP:{entry_spot}", "CRITICAL", "ENTRY GATE MAPPING REQUIRED",
            f"No healthy simulator barrier can be associated with {entry_spot}.",
            sim_time, plate, entry_api_zone
        )
        return

    gate = candidates[0]
    reserve_spot(spot, plate, sim_time)
    upsert_car(
        plate,
        assigned_spot=spot,
        status="WAITING_ENTRY",
        decision=f"Assigned {spot} in {spot_zone}; entry candidate {gate}"
    )

    item = {
        "plate": plate,
        "spot": spot,
        "entry_spot": entry_spot,
        "entry_zone": spot_zone,
        "gate_candidates": candidates,
        "tried_gates": [],
        "sent": False,
        "route_sent_at": None,
    }

    audit(
        "system", "ENTRY_ASSIGN", plate,
        f"{entry_spot} -> gate candidates={candidates}; assigned={spot}",
        "OK", sim_time
    )

    with state_lock:
        entry_queues[gate].append(item)
    process_entry_gate(gate)



# ============================================================
# EXIT + PAYMENT: NO PAYMENT = NO GATE

# ============================================================
# EXIT + PAYMENT: NO PAYMENT = NO GATE
# ============================================================
def exit_capacity():
    conn = db()
    rows = conn.execute(
        "SELECT raw_json FROM components WHERE kind='spot'"
    ).fetchall()
    conn.close()
    count = 0
    for r in rows:
        try:
            raw = json.loads(r["raw_json"] or "{}")
        except Exception:
            raw = {}
        if str(raw.get("purpose") or "") == "ExitSpot":
            count += 1
    return max(1, count)


def schedule_exit(plate, planned_minutes):
    if plate in scheduled_exit_plates:
        return
    scheduled_exit_plates.add(plate)
    seconds = max(1, int(planned_minutes or 1)) * 60

    def later():
        with state_lock:
            if plate not in exit_request_queue:
                exit_request_queue.append(plate)
        dispatch_exit_requests()

    t = threading.Timer(seconds, later)
    t.daemon = True
    t.start()


def api_exit_sensors_for_zone(zone_name):
    """Return healthy ExitSpot sensors whose zoneParent is exposed by the API."""
    conn = db()
    rows = [dict(r) for r in conn.execute(
        """SELECT * FROM components
           WHERE kind='spot' AND zone=?
             AND broken=0 AND under_maintenance=0""",
        (zone_name,)
    ).fetchall()]
    conn.close()

    result = []
    for row in rows:
        try:
            raw = json.loads(row.get("raw_json") or "{}")
        except Exception:
            raw = {}
        if str(raw.get("purpose") or "") == "ExitSpot":
            result.append(row["name"])
    return sorted(result)


def dispatch_exit_requests():
    # Cars remain parked while ANY barrier is broken or under maintenance.
    # Keep exit_request_queue intact; resume_after_gate_lockdown() restarts it.
    if not gate_traffic_allowed(refresh_live=True):
        return

    capacity = exit_capacity()
    conn = db()
    active = conn.execute(
        """SELECT COUNT(*) AS n FROM cars
           WHERE status IN(
             'TO_EXIT','AT_EXIT','PAYMENT_PENDING','PAYMENT_HOLD','PAID_WAITING_GATE'
           )"""
    ).fetchone()["n"]
    conn.close()

    while active < capacity:
        with state_lock:
            if not exit_request_queue:
                break
            plate = exit_request_queue.pop(0)

        car = get_car(plate)
        if not car or car.get("status") != "PARKED":
            continue

        actual_spot = car.get("actual_spot") or car.get("assigned_spot")
        spot_row = component_row(actual_spot, "spot") if actual_spot else None
        zone = (
            (spot_row or {}).get("zone")
            or car.get("entry_zone")
            or ""
        )
        exit_candidates = api_exit_sensors_for_zone(zone)
        target_exit = exit_candidates[0] if exit_candidates else None

        if not target_exit:
            upsert_alert(
                f"EXIT_ROUTE:{plate}",
                "HIGH", "EXIT ROUTE FAILURE",
                f"The simulator API exposes no healthy ExitSpot for parking zone {zone!r}.",
                simulator_now(), plate, zone
            )
            continue

        exit_row = component_row(target_exit, "spot")
        if exit_row and (
            int(exit_row.get("broken") or 0)
            or int(exit_row.get("under_maintenance") or 0)
        ):
            upsert_alert(
                f"EXIT_ROUTE:{plate}",
                "CRITICAL", "EXIT SENSOR UNAVAILABLE",
                f"{target_exit} is broken or under maintenance.",
                simulator_now(), plate, zone, target_exit
            )
            continue

        try:
            send_car(plate, target_exit)
            upsert_car(
                plate,
                status="TO_EXIT",
                exit_spot=target_exit,
                exit_zone=zone
            )
            active += 1
        except Exception as e:
            upsert_alert(
                f"EXIT_ROUTE:{plate}", "HIGH", "EXIT ROUTE FAILURE",
                str(e), simulator_now(), plate
            )
            break


def calculate_charge(plate, exit_sim_time):
    """
    Authoritative Level 2 billing.

    TIME:
      billable parking duration = physical Park/CarIn timestamp
                                  -> physical ExitSpot/CarIn timestamp

    MONEY:
      Normal/Accessible total = minutes * PARKING_RATE_PER_MIN
      Electric total          = Normal price * 2

    The /car/{plate}/charge API accepts parkingCost and chargingCost
    separately, so for Electric cars the second half of the 2x total is
    sent as chargingCost. There is no invented extra multiplier on top.
    """
    car = get_car(plate)
    if not car:
        return 0, 1, float(PARKING_RATE_PER_MIN), 0.0

    parked_time = car.get("parked_time")
    seconds = sim_seconds_between(parked_time, exit_sim_time)

    # A physical trip that reaches ExitSpot in <60s is still billed as 1 min.
    # If a timestamp is missing, use planned_minutes only as a defensive
    # fallback; normal operation always uses simulator timestamps.
    if parked_time and exit_sim_time:
        minutes = max(1, math.ceil(seconds / 60))
    else:
        minutes = max(1, int(car.get("planned_minutes") or 1))

    base_parking = round(minutes * PARKING_RATE_PER_MIN, 2)
    is_electric = "electric" in str(car.get("car_type") or "").lower()

    if is_electric:
        total = round(base_parking * ELECTRIC_TOTAL_MULTIPLIER, 2)
        charging = round(total - base_parking, 2)
    else:
        charging = 0.0

    return seconds, minutes, base_parking, charging


def gate_has_unpaid_blocker(gate):
    """
    Return an unpaid vehicle only when it is physically FIRST at an ExitSpot
    associated with this gate. Cars queued behind the front vehicle do not
    incorrectly block a paid front vehicle.
    """
    with state_lock:
        fronts = [
            queue[0]
            for queue in physical_exit_queues.values()
            if queue
        ]

    for plate in fronts:
        car = get_car(plate) or {}
        if car.get("exit_gate") != gate:
            continue
        if car.get("payment_status") != "PAID":
            return plate

    return None




def _exit_candidates_for_plate(plate):
    car = get_car(plate) or {}
    sensor = car.get("exit_spot")
    if not sensor:
        return []
    return [gate for _, gate, _ in candidate_gates_for_sensor(sensor, "exit")]


def _queue_paid_exit_on_next_gate(plate, failed_gate, reason):
    car = get_car(plate) or {}
    candidates = _exit_candidates_for_plate(plate)
    tried = set()

    with lane_trial_lock:
        raw = get_state(f"exit_tried:{plate}", "")
        if raw:
            tried.update(x for x in raw.split(",") if x)
        tried.add(failed_gate)
        set_state(f"exit_tried:{plate}", ",".join(sorted(tried)))

    next_gate = next((g for g in candidates if g not in tried), None)
    if not next_gate:
        upsert_car(plate, status="PAID_EXIT_HOLD")
        upsert_alert(
            f"EXIT_DISCOVERY:{plate}",
            "CRITICAL", "EXIT LANE DISCOVERY FAILED",
            f"Payment is valid, but no exit barrier produced a physical departure. "
            f"Tried {sorted(tried)}. Last reason: {reason}. Vehicle remains held.",
            simulator_now(), plate,
            car.get("exit_zone") or ""
        )
        return

    upsert_car(plate, exit_gate=next_gate, status="PAID_WAITING_GATE")
    audit(
        "system", "EXIT_GATE_DISCOVERY_RETRY", plate,
        f"{failed_gate} -> {next_gate}; reason={reason}",
        "RETRY", simulator_now()
    )
    with state_lock:
        paid_exit_queues[next_gate].insert(0, plate)
    process_paid_exit_gate(next_gate)


def delayed_paid_exit_release(gate, plate):
    """
    Safety fallback for a late/missed exit-gate webhook.

    Payment alone is NOT enough. The selected barrier must also be confirmed
    fully Open by the simulator API before goto/leavepark is sent.
    """
    time.sleep(ROUTE_RELEASE_FALLBACK_SEC)

    with state_lock:
        item = exit_active.get(gate)
        if not item or item.get("plate") != plate or item.get("sent"):
            return

    car = get_car(plate) or {}
    if car.get("payment_status") != "PAID":
        return

    if not gate_traffic_allowed(refresh_live=True):
        with state_lock:
            exit_active.pop(gate, None)
        upsert_car(
            plate,
            status="PAID_GATE_LOCKDOWN",
            decision="Global barrier lockdown before delayed exit release"
        )
        return

    if not gate_physically_open(gate):
        audit(
            "system",
            "PAID_EXIT_RELEASE_BLOCKED_GATE_NOT_OPEN",
            plate,
            f"{gate} is not confirmed Open; paid car remains held.",
            "BLOCKED",
            simulator_now()
        )
        return

    try:
        latest = get_car(plate) or {}
        if latest.get("payment_status") != "PAID":
            audit(
                "system",
                "EXIT_RELEASE_BLOCKED_UNPAID",
                plate,
                "leavepark command blocked because payment_status is not PAID.",
                "BLOCKED",
                simulator_now()
            )
            return

        send_car(plate, "leavepark")
        with state_lock:
            current = exit_active.get(gate)
            if current and current.get("plate") == plate:
                current["sent"] = True

        audit(
            "system",
            "PAID_EXIT_RELEASE_API_CONFIRMED",
            plate,
            f"{gate}=Open confirmed by simulator API; leavepark sent.",
            "OK",
            simulator_now()
        )
    except Exception as e:
        audit(
            "system",
            "PAID_EXIT_RELEASE_API_CONFIRMED",
            plate,
            str(e),
            "ERROR",
            simulator_now()
        )


def exit_lane_discovery_watch(gate, plate):
    """
    Exit gate lifecycle:

      payment must be PAID
        -> request Open
        -> wait for API state == Open
        -> send leavepark
        -> KEEP THIS GATE OPEN
        -> wait as long as necessary for real ExitSpot CarOut
        -> only CarOut may restore/close the gate

    An unpaid vehicle never reaches this release lifecycle.
    """
    started = time.time()

    while True:
        time.sleep(0.5)

        car = get_car(plate) or {}

        # Hard payment interlock remains true throughout the entire crossing.
        if car.get("payment_status") != "PAID":
            with state_lock:
                current = exit_active.get(gate)
                if current and current.get("plate") == plate:
                    exit_active.pop(gate, None)

            try:
                auto_restore_gate_api_baseline(gate, "system", simulator_now())
            except Exception:
                pass

            upsert_alert(
                f"EXIT_PAYMENT_INTERLOCK:{plate}",
                "CRITICAL",
                "EXIT RELEASE CANCELLED",
                "Vehicle is not PAID. Exit release cancelled and vehicle remains blocked.",
                simulator_now(),
                plate,
                car.get("exit_zone") or "",
                gate
            )
            return

        if car.get("departure_time") or car.get("status") == "LEFT":
            return

        with state_lock:
            current = exit_active.get(gate)
            if not current or current.get("plate") != plate:
                return
            sent = bool(current.get("sent"))

        if sent:
            # PAID vehicle has received leavepark. Keep gate open until the
            # real ExitSpot CarOut webhook confirms it physically left.
            row = component_row(gate, "gate") or {}
            if int(row.get("broken") or 0) or int(row.get("under_maintenance") or 0):
                upsert_alert(
                    f"EXIT_GATE_FAILED_DURING_CROSSING:{plate}",
                    "CRITICAL",
                    "EXIT GATE FAILED DURING CROSSING",
                    f"{gate} became unavailable while paid vehicle {plate} was leaving.",
                    simulator_now(),
                    plate,
                    car.get("exit_zone") or "",
                    gate
                )
                continue

            live_state = refresh_live_gate_state(gate).strip().lower()
            if live_state not in ("open", "opening"):
                try:
                    component_action("gate", gate, "open", "system", simulator_now())
                    audit(
                        "system",
                        "EXIT_GATE_HOLD_OPEN",
                        plate,
                        f"Re-opened {gate}; waiting for real ExitSpot CarOut.",
                        "OK",
                        simulator_now()
                    )
                except Exception as e:
                    audit(
                        "system",
                        "EXIT_GATE_HOLD_OPEN",
                        plate,
                        str(e),
                        "ERROR",
                        simulator_now()
                    )
            continue

        # PAID but never released through this candidate barrier.
        if time.time() - started < LANE_DISCOVERY_TIMEOUT_SEC:
            continue

        with state_lock:
            current = exit_active.get(gate)
            if not current or current.get("plate") != plate:
                return
            if current.get("sent"):
                continue
            exit_active.pop(gate, None)

        try:
            auto_restore_gate_api_baseline(gate, "system", simulator_now())
        except Exception:
            pass

        _queue_paid_exit_on_next_gate(
            plate,
            gate,
            f"{gate} never produced a confirmed Open release within "
            f"{LANE_DISCOVERY_TIMEOUT_SEC:.1f}s"
        )
        return


def process_paid_exit_gate(gate):
    with state_lock:
        if gate in exit_active or not paid_exit_queues[gate]:
            return
        plate = paid_exit_queues[gate][0]

    car = get_car(plate) or {}

    if not gate_traffic_allowed(refresh_live=True):
        upsert_car(
            plate,
            status="PAID_GATE_LOCKDOWN",
            decision="Global barrier lockdown before exit release"
        )
        return

    # Gate release is permitted only for the physically-front PAID vehicle.
    exit_spot = car.get("exit_spot")
    if (
        car.get("payment_status") != "PAID"
        or not exit_spot
        or physical_exit_front(exit_spot) != plate
    ):
        return

    with state_lock:
        # Re-check under lock then consume from paid-ready queue.
        if gate in exit_active or not paid_exit_queues[gate]:
            return
        if paid_exit_queues[gate][0] != plate:
            return
        paid_exit_queues[gate].pop(0)
        exit_active[gate] = {"plate": plate, "sent": False}

    upsert_car(plate, status="PAID_WAITING_GATE", exit_gate=gate)

    try:
        gate_row = component_row(gate, "gate") or {}
        if int(gate_row.get("broken") or 0):
            raise RuntimeError(f"{gate} is broken")
        if int(gate_row.get("under_maintenance") or 0):
            raise RuntimeError(f"{gate} is under maintenance")

        if gate_physically_open(gate):
            latest = get_car(plate) or {}
            if latest.get("payment_status") != "PAID":
                raise RuntimeError(
                    f"Payment interlock blocked exit for {plate}: "
                    f"status={latest.get('payment_status')}"
                )
            send_car(plate, "leavepark")
            with state_lock:
                if gate in exit_active:
                    exit_active[gate]["sent"] = True
            audit(
                "system",
                "PAID_EXIT_FAST_PATH",
                plate,
                f"{gate}=Open confirmed by simulator API; released physically-front PAID vehicle.",
                "OK",
                simulator_now()
            )
        else:
            component_action("gate", gate, "open", "system", simulator_now())

        threading.Thread(
            target=delayed_paid_exit_release,
            args=(gate, plate),
            daemon=True
        ).start()

        threading.Thread(
            target=exit_lane_discovery_watch,
            args=(gate, plate),
            daemon=True
        ).start()

    except Exception as e:
        with state_lock:
            exit_active.pop(gate, None)

        # Keep payment accepted. Try another REAL healthy barrier candidate
        # for this ExitSpot rather than asking the car to pay again.
        _queue_paid_exit_on_next_gate(plate, gate, str(e))




def verify_payment(plate, received_amount):
    """
    Multi-factor verification copied from the supplied reference payment flow:

      1) vehicle must be tracked from entry
      2) amount must exactly match PARKMIND's stored expected amount
      3) payment must not already have been accepted
      4) vehicle must be in a legitimate exit/payment state
    """
    car = get_car(plate)
    if not car:
        return False, "GHOST_CAR"

    factors = []

    if not car.get("entry_time"):
        return False, "GHOST_CAR_NO_ENTRY"
    factors.append("TRACKED=OK")

    expected = float(car.get("expected_amount") or 0)
    received_amount = float(received_amount or 0)

    if abs(received_amount - expected) > 0.001:
        if received_amount < expected:
            return (
                False,
                f"INSUFFICIENT_FUNDS(expected={expected}, received={received_amount})"
            )
        return False, f"AMOUNT_MISMATCH(expected={expected}, received={received_amount})"
    factors.append(f"AMOUNT_MATCH=OK({expected})")

    if car.get("payment_status") == "PAID":
        return False, "DOUBLE_PAYMENT"
    factors.append("NOT_DOUBLE=OK")

    allowed = (
        "AT_EXIT",
        "PAYMENT_PENDING",
        "WAITING_TO_CHARGE",
        "PAYMENT_HOLD",
        "TO_EXIT",
        "CHARGE_ERROR",
    )
    if car.get("status") not in allowed:
        return False, f"BAD_STATUS({car.get('status')})"
    factors.append("STATUS_OK=OK")

    return True, " | ".join(factors)


def physical_exit_front(exit_spot):
    with state_lock:
        queue = physical_exit_queues.get(exit_spot) or []
        return queue[0] if queue else None


def add_to_physical_exit_queue(exit_spot, plate):
    with state_lock:
        queue = physical_exit_queues[exit_spot]
        if plate not in queue:
            queue.append(plate)
        return queue[0] == plate


def remove_from_physical_exit_queue(exit_spot, plate):
    with state_lock:
        queue = physical_exit_queues.get(exit_spot) or []
        if plate in queue:
            queue.remove(plate)
        next_plate = queue[0] if queue else None
        if not queue and exit_spot in physical_exit_queues:
            physical_exit_queues.pop(exit_spot, None)
        return next_plate


def request_payment_now(plate):
    """
    Send the simulator charge command using the already-calculated amounts.
    Does not release any gate.
    """
    car = get_car(plate) or {}
    if car.get("payment_status") == "PAID":
        return

    parking_cost = float(car.get("parking_cost") or 0)
    charging_cost = float(car.get("charging_cost") or 0)

    upsert_car(
        plate,
        payment_status="REQUESTED",
        status="PAYMENT_PENDING"
    )
    charge_car(plate, parking_cost, charging_cost)


def payment_retry_watch(plate):
    """
    Reference behaviour:
    while the vehicle remains in the exit/payment flow and is not PAID,
    retry the legitimate simulator charge request after a short delay.

    A valid payment webhook stops this loop automatically.
    """
    time.sleep(EXIT_PAYMENT_RETRY_SEC)

    car = get_car(plate)
    if not car:
        return
    if car.get("payment_status") == "PAID":
        return
    if car.get("departure_time"):
        return

    if car.get("status") not in (
        "AT_EXIT",
        "PAYMENT_PENDING",
        "PAYMENT_HOLD",
        "TO_EXIT",
        "CHARGE_ERROR",
    ):
        return

    audit(
        "system",
        "PAYMENT_RETRY",
        plate,
        "Vehicle still unpaid in exit flow; retrying simulator charge request.",
        "RETRY",
        simulator_now()
    )

    try:
        request_payment_now(plate)
    except Exception as e:
        upsert_car(
            plate,
            payment_status="CHARGE_ERROR",
            status="PAYMENT_HOLD"
        )
        car = get_car(plate) or {}
        upsert_alert(
            f"PAYMENT:{plate}",
            "HIGH",
            "PAYMENT REQUEST FAILED",
            str(e),
            simulator_now(),
            plate,
            car.get("exit_zone") or "",
            car.get("exit_gate") or ""
        )

    threading.Thread(
        target=payment_retry_watch,
        args=(plate,),
        daemon=True
    ).start()


def queue_paid_vehicle_for_release(plate):
    """
    Only the FRONT vehicle of its physical ExitSpot queue may enter the
    gate-release stage. This is the key behaviour from the reference system.
    """
    car = get_car(plate) or {}
    if car.get("payment_status") != "PAID":
        return False

    if not gate_traffic_allowed(refresh_live=True):
        upsert_car(
            plate,
            status="PAID_GATE_LOCKDOWN",
            decision="Payment accepted; exit blocked until every barrier is healthy"
        )
        return False

    exit_spot = car.get("exit_spot")
    if not exit_spot:
        return False

    if physical_exit_front(exit_spot) != plate:
        audit(
            "system",
            "PAID_WAITING_PHYSICAL_QUEUE",
            plate,
            f"Paid, but waiting behind the front vehicle at {exit_spot}.",
            "WAIT",
            simulator_now()
        )
        upsert_car(plate, status="PAID_WAITING_GATE")
        return False

    gate = car.get("exit_gate")
    if not gate:
        candidates = [
            g for _, g, _ in candidate_gates_for_sensor(exit_spot, "exit")
        ]
        gate = candidates[0] if candidates else None
        if gate:
            upsert_car(plate, exit_gate=gate)

    if not gate:
        upsert_alert(
            f"EXIT_NO_GATE:{plate}",
            "CRITICAL",
            "PAID CAR HAS NO EXIT GATE",
            "Payment is valid, but no healthy simulator barrier candidate is available for this ExitSpot.",
            simulator_now(),
            plate,
            car.get("exit_zone") or ""
        )
        return False

    with state_lock:
        if plate not in paid_exit_queues[gate]:
            paid_exit_queues[gate].append(plate)

    process_paid_exit_gate(gate)
    return True


def continue_physical_exit_queue(exit_spot):
    """
    After the front car physically leaves, immediately continue with the next
    car at that SAME ExitSpot, exactly like the supplied reference flow.
    """
    next_plate = physical_exit_front(exit_spot)
    if not next_plate:
        return

    car = get_car(next_plate) or {}

    if car.get("payment_status") == "PAID":
        queue_paid_vehicle_for_release(next_plate)
        return

    # If the next car is still unpaid, do NOT release it. Nudge its payment
    # request and leave the physical queue blocked behind that vehicle.
    try:
        request_payment_now(next_plate)
    except Exception as e:
        upsert_car(
            next_plate,
            payment_status="CHARGE_ERROR",
            status="PAYMENT_HOLD"
        )
        upsert_alert(
            f"PAYMENT:{next_plate}",
            "HIGH",
            "PAYMENT REQUEST FAILED",
            str(e),
            simulator_now(),
            next_plate,
            car.get("exit_zone") or "",
            car.get("exit_gate") or ""
        )


# ============================================================
# CO SAFETY + ENERGY POLICY
# ============================================================

def danger_truthy(value):
    # Simulator CO event levels include Safe, Mid, High, Critical.
    # Fans should run from Mid upward (CO is already >= the documented 50 threshold).
    return str(value).strip().lower() in (
        "1", "true", "danger", "mid", "moderate",
        "high", "critical", "yes", "unsafe"
    )


def fans_in_zone(zone):
    conn = db()
    rows = [dict(r) for r in conn.execute(
        """SELECT * FROM components
           WHERE kind='fan' AND zone=?""",
        (zone,)
    ).fetchall()]
    conn.close()
    return rows


def handle_co(zone, level, danger, sim_time):
    conn = db()
    conn.execute(
        """INSERT INTO zones(name,co_level,danger,last_update)
           VALUES(?,?,?,?)
           ON CONFLICT(name) DO UPDATE SET
             co_level=excluded.co_level,
             danger=excluded.danger,
             last_update=excluded.last_update""",
        (zone, level, str(danger), sim_time)
    )
    conn.commit()
    conn.close()

    unsafe = danger_truthy(danger)
    fans = fans_in_zone(zone)

    if unsafe:
        upsert_alert(
            f"CO:{zone}", "CRITICAL", "HIGH CO LEVEL",
            f"Simulator reported CO={level}, DangerLevel={danger}.",
            sim_time, zone=zone
        )
        healthy = [f for f in fans if not f["broken"] and not f["under_maintenance"]]
        if not healthy:
            upsert_alert(
                f"CO_NO_FAN:{zone}", "CRITICAL", "NO HEALTHY EXHAUST FAN",
                "CO danger is active but no healthy exhaust fan is available.",
                sim_time, zone=zone
            )
        for fan in healthy:
            if str(fan["state"]).lower() != "on":
                try:
                    component_action("fan", fan["name"], "on", "system", sim_time)
                except Exception as e:
                    upsert_alert(
                        f"FAN_START:{fan['name']}", "CRITICAL", "FAN START FAILURE",
                        str(e), sim_time, zone=zone, component=fan["name"]
                    )
    else:
        resolve_alert(f"CO:{zone}", sim_time)
        resolve_alert(f"CO_NO_FAN:{zone}", sim_time)
        for fan in fans:
            if str(fan["state"]).lower() == "on":
                try:
                    component_action("fan", fan["name"], "off", "system", sim_time)
                except Exception:
                    pass


def simulator_is_day(sim_time):
    dt = parse_sim_time(sim_time)
    if not dt:
        return None
    return DAY_START_HOUR <= dt.hour < DAY_END_HOUR


def zone_has_activity(zone):
    conn = db()
    occupied = conn.execute(
        """SELECT COUNT(*) AS n FROM components
           WHERE kind='spot' AND zone=? AND state='Occupied'""",
        (zone,)
    ).fetchone()["n"]
    active = conn.execute(
        """SELECT COUNT(*) AS n FROM cars
           WHERE departure_time IS NULL
             AND (entry_zone=? OR exit_zone=?)
             AND status NOT IN('LEFT','NO_SAFE_SPACE')""",
        (zone, zone)
    ).fetchone()["n"]
    conn.close()
    return (occupied + active) > 0


def apply_light_policy(sim_time):
    is_day = simulator_is_day(sim_time)
    if is_day is None:
        return

    conn = db()
    lights = [dict(r) for r in conn.execute(
        "SELECT * FROM components WHERE kind='light'"
    ).fetchall()]
    conn.close()

    for light in lights:
        if light["broken"] or light["under_maintenance"]:
            continue
        target_on = (not is_day) and zone_has_activity(light["zone"])
        current_on = str(light["state"]).lower() == "on"
        if target_on == current_on:
            continue
        try:
            component_action(
                "light", light["name"],
                "on" if target_on else "off",
                "system", sim_time
            )
        except Exception:
            pass


# ============================================================
# PREVENTIVE MAINTENANCE
# ============================================================
def component_health(row):
    """
    Returns ONLY simulator-backed life percentage when available.

    None means the simulator API did not expose a life percentage or enough
    API fields to calculate one. Never manufacture a percentage from local
    policy thresholds.
    """
    value = row.get("api_life_pct")
    return float(value) if value is not None else None


def safe_for_maintenance(row):
    """Safety gate before any repair command is sent."""
    kind = row["kind"]

    if int(row.get("under_maintenance") or 0):
        return False, "Already under maintenance"

    if kind == "spot":
        conn = db()
        reserved = conn.execute(
            "SELECT 1 FROM reservations WHERE spot=?",
            (row["name"],)
        ).fetchone()
        conn.close()
        if str(row.get("state") or "").lower() == "occupied":
            return False, "Parking spot is occupied"
        if reserved:
            return False, "Parking spot is reserved for an incoming vehicle"
        return True, "Spot is free and unreserved"

    if kind == "gate":
        with state_lock:
            busy = row["name"] in entry_active or row["name"] in exit_active
        if busy:
            return False, "Gate is handling an active vehicle"
        if int(row.get("broken") or 0):
            return True, "Broken gate is idle; corrective repair can start"
        if str(row.get("state") or "").lower() != "closed":
            return False, "Close and idle the gate before preventive repair"
        return True, "Gate is closed and idle"

    if kind == "fan":
        # A broken fan should be repaired urgently even during CO danger because
        # it is already unavailable. A healthy fan should not be taken offline
        # for preventive work while its zone needs ventilation.
        if int(row.get("broken") or 0):
            return True, "Broken fan should be restored immediately"
        conn = db()
        z = conn.execute(
            "SELECT danger FROM zones WHERE name=?",
            (row.get("zone") or "",)
        ).fetchone()
        conn.close()
        if z and danger_truthy(z["danger"]):
            return False, "Zone CO requires this ventilation capacity"
        if str(row.get("state") or "").lower() == "on":
            return False, "Turn fan off before preventive maintenance"
        return True, "Fan is off and zone is not in a CO incident"

    if kind == "light":
        return False, "Simulator documentation exposes no light repair endpoint"

    return False, "Unsupported component type"


def evaluate_preventive_maintenance(sim_time):
    """
    Event-driven recommendation refresh.

    Preventive-maintenance due status comes from /list-alarms, not from a fake
    locally invented life percentage. This function only enriches the alert
    with whether a safe maintenance window exists right now.
    """
    conn = db()
    rows = [dict(r) for r in conn.execute(
        """SELECT * FROM components
           WHERE maintenance_required=1
             AND broken=0"""
    ).fetchall()]
    conn.close()

    for row in rows:
        safe, why = safe_for_maintenance(row)
        life = component_health(row)
        life_text = f"{life:.1f}% simulator life remaining" if life is not None else "life % not exposed by simulator API"
        upsert_alert(
            f"MAINT_DUE:{row['name']}",
            "HIGH",
            "SIMULATOR MAINTENANCE REQUIRED",
            f"{row.get('maintenance_problem') or 'Require Maintenance'}; "
            f"{life_text}; {'REPAIR NOW' if safe else 'WAIT'} — {why}.",
            sim_time,
            zone=row.get("zone") or "",
            component=row["name"]
        )


# ============================================================
# EVENT HANDLING
# ============================================================
def find_component_kind(name):
    conn = db()
    row = conn.execute(
        "SELECT kind FROM components WHERE name=? LIMIT 1",
        (name,)
    ).fetchone()
    conn.close()
    return row["kind"] if row else None


def handle_gate_action(data, sim_time):
    name = data.get("Name")
    action = str(data.get("Action") or "")
    if not name:
        return

    row = component_row(name, "gate")
    previous = str(row.get("state") or "") if row else ""
    cycle_add = 1 if action in ("Opening", "Open") and previous not in ("Opening", "Open") else 0
    update_component_state(name, "gate", action, cycle_add=cycle_add, sim_time=sim_time)

    with state_lock:
        incoming = entry_active.get(name)
        outgoing = exit_active.get(name)

    if incoming and action == "Open" and not incoming["sent"]:
        try:
            # Even a physically Open gate cannot admit a car while ANY
            # simulator barrier is broken/under maintenance.
            if not gate_traffic_allowed(refresh_live=True, sim_time=sim_time):
                plate = incoming["plate"]
                release_reservation(plate=plate)
                with state_lock:
                    entry_active.pop(name, None)
                upsert_car(
                    plate,
                    assigned_spot=None,
                    status="GATE_LOCKDOWN_ENTRY",
                    decision="Global barrier lockdown at gate-open event"
                )
                return

            target_zone = target_zone_for_spot(incoming["spot"])

            # Gate webhook says fully Open. Re-check live API before releasing.
            if not gate_physically_open(name):
                audit(
                    "system",
                    "ENTRY_RELEASE_BLOCKED_GATE_NOT_OPEN",
                    incoming["plate"],
                    f"{name} webhook reported Open but /list-barriers did not confirm Open.",
                    "BLOCKED",
                    sim_time
                )
                return
            if not zone_access_is_open(target_zone):
                prepare_zone_access_for_car(
                    incoming["plate"], incoming["spot"]
                )
                audit(
                    "system", "ENTRY_WAITING_FOR_ZONE_ACCESS",
                    incoming["plate"],
                    f"entry gate open; waiting for target_zone={target_zone}",
                    "WAIT", sim_time
                )
                return

            send_car(incoming["plate"], incoming["spot"])
            with state_lock:
                if name in entry_active:
                    entry_active[name]["sent"] = True
            upsert_car(incoming["plate"], status="TO_SPOT")
        except Exception as e:
            upsert_alert(
                f"ENTRY_RELEASE:{name}", "CRITICAL", "ENTRY RELEASE FAILURE",
                str(e), sim_time, incoming["plate"], component=name
            )

    if outgoing and action == "Open" and not outgoing["sent"]:
        try:
            if not gate_traffic_allowed(refresh_live=True, sim_time=sim_time):
                plate = outgoing["plate"]
                with state_lock:
                    exit_active.pop(name, None)
                upsert_car(
                    plate,
                    status="PAID_GATE_LOCKDOWN",
                    decision="Global barrier lockdown at exit gate-open event"
                )
                return

            if not gate_physically_open(name):
                audit(
                    "system",
                    "PAID_EXIT_RELEASE_BLOCKED_GATE_NOT_OPEN",
                    outgoing["plate"],
                    f"{name} webhook reported Open but /list-barriers did not confirm Open.",
                    "BLOCKED",
                    sim_time
                )
                return
            latest = get_car(outgoing["plate"]) or {}
            if latest.get("payment_status") != "PAID":
                audit(
                    "system",
                    "EXIT_RELEASE_BLOCKED_UNPAID",
                    outgoing["plate"],
                    f"{name}=Open but payment_status={latest.get('payment_status')}; no leavepark command sent.",
                    "BLOCKED",
                    sim_time
                )
                return

            send_car(outgoing["plate"], "leavepark")
            with state_lock:
                if name in exit_active:
                    exit_active[name]["sent"] = True
        except Exception as e:
            upsert_alert(
                f"EXIT_RELEASE:{name}", "CRITICAL", "EXIT RELEASE FAILURE",
                str(e), sim_time, outgoing["plate"], component=name
            )


def handle_car_event(data, sim_time):
    plate = str(data.get("CarPlateNumber") or "").strip()
    car_type = data.get("CarType") or "Normal"
    spot_name = data.get("SpotName")
    spot_type = data.get("SpotType")
    direction = data.get("Direction")
    planned = int(data.get("PlannedParkingDurationInMinutes") or 0)

    if not plate or not spot_name:
        return

    zone = sensor_zone(spot_name)

    if spot_type == "EntrySpot" and direction == "CarIn":
        enqueue_arrival(plate, car_type, planned, spot_name, sim_time)
        return

    if spot_type == "EntrySpot" and direction == "CarOut":
        car = get_car(plate) or {}
        active_gate = find_active_entry_gate(plate)

        # IMPORTANT:
        # EntrySpot CarOut can also happen when PARKMIND intentionally rejects
        # a car with goto/leavepark. That is NOT proof that an entrance barrier
        # worked. Only learn/close a gate when this plate actually had an active
        # parking-entry gate trial and an assigned parking bay.
        successful_entry_crossing = bool(
            active_gate
            and car.get("assigned_spot")
            and car.get("status") not in ("NO_SAFE_SPACE", "ENTRY_HOLD")
        )

        if successful_entry_crossing:
            gate = active_gate
            remember_lane_mapping(
                spot_name, "entry", gate,
                confidence="CONFIRMED",
                source="EntrySpot CarOut during parking admission"
            )
            resolve_alert(f"ENTRY_MAP:{spot_name}", sim_time)
            resolve_alert(f"ENTRY_DISCOVERY:{plate}", sim_time)

            with state_lock:
                if gate in entry_active and entry_active[gate].get("plate") == plate:
                    entry_active.pop(gate, None)
                has_next = bool(entry_queues[gate])

            upsert_car(plate, status="TO_SPOT")

            if has_next:
                process_entry_gate(gate)
            else:
                threading.Thread(
                    target=delayed_restore_entry_gate_if_idle,
                    args=(gate,),
                    daemon=True
                ).start()

        else:
            audit(
                "system", "ENTRY_SENSOR_CAROUT_NO_GATE_CONFIRM",
                plate,
                f"{spot_name} CarOut observed with status={car.get('status')}; "
                "not treated as proof of an entrance-gate mapping.",
                "IGNORED_FOR_MAPPING",
                sim_time
            )

        return

    if spot_type == "Park":
        row = component_row(spot_name, "spot")
        if direction == "CarIn":
            update_component_state(
                spot_name, "spot", "Occupied",
                cycle_add=1, sim_time=sim_time
            )
            release_reservation(spot=spot_name)
            car = get_car(plate)
            if not car:
                # Manual/untracked parking detection.
                upsert_car(
                    plate, car_type=car_type, actual_spot=spot_name,
                    parked_time=sim_time, status="UNREGISTERED_PARKED"
                )
                upsert_alert(
                    f"UNREGISTERED_PARK:{plate}", "CRITICAL",
                    "UNREGISTERED VEHICLE PARKED",
                    "Vehicle was detected in a parking bay without a trusted entry record.",
                    sim_time, plate, zone
                )
                return

            expected = car.get("assigned_spot")
            if expected and expected != spot_name:
                # Do not fabricate route history. Adopt only if actual simulator sensor says car is there.
                release_reservation(plate=plate)
                upsert_alert(
                    f"WRONG_SPOT:{plate}", "HIGH", "VEHICLE PARKED IN DIFFERENT SPOT",
                    f"Assigned {expected}, simulator detected {spot_name}. Actual sensor state is preserved.",
                    sim_time, plate, zone
                )

            upsert_car(
                plate, actual_spot=spot_name, assigned_spot=spot_name,
                parked_time=sim_time, status="PARKED"
            )
            schedule_exit(plate, planned or car.get("planned_minutes") or 1)

            # Physical Park CarIn proves the car reached the target zone/bay.
            # Target-zone barriers may now return to their API startup state,
            # unless another in-flight car still needs the same zone.
            release_zone_access_for_car(plate, zone)
        else:
            update_component_state(spot_name, "spot", "Free", sim_time=sim_time)
        return

    if spot_type == "ExitSpot" and direction == "CarIn":
        ensure_level2_topology_ready(f"exit:{spot_name}")

        car = get_car(plate)
        exit_candidates = [
            g for _, g, _ in candidate_gates_for_sensor(spot_name, "exit")
        ]
        gate = exit_candidates[0] if exit_candidates else None
        set_state(f"exit_tried:{plate}", "")

        # Physical queue first. Multiple Level 2 ExitSpots each get their own
        # queue, so one exit lane does not falsely serialize every zone.
        is_front = add_to_physical_exit_queue(spot_name, plate)

        # Manual/untracked car: never invent a payment history.
        if not car or not car.get("entry_time"):
            upsert_car(
                plate,
                car_type=car_type,
                exit_arrival_time=sim_time,
                exit_spot=spot_name,
                exit_zone=zone,
                exit_gate=gate,
                payment_status="BLOCKED",
                status="UNREGISTERED_EXIT"
            )
            upsert_alert(
                f"UNREGISTERED_EXIT:{plate}",
                "CRITICAL",
                "UNREGISTERED VEHICLE AT EXIT",
                "No trusted entry timestamp exists. Vehicle remains blocked for operator/security verification.",
                sim_time,
                plate,
                zone,
                gate or ""
            )
            return

        # If a valid payment somehow arrived before this CarIn was processed,
        # the front vehicle may continue immediately.
        if car.get("payment_status") == "PAID":
            upsert_car(
                plate,
                exit_arrival_time=sim_time,
                exit_spot=spot_name,
                exit_zone=zone,
                exit_gate=gate,
                status="PAID_WAITING_GATE"
            )
            if is_front:
                queue_paid_vehicle_for_release(plate)
            return

        # Duplicate/repeated ExitSpot CarIn webhook: do not double-calculate
        # or double-charge. The existing retry watcher owns payment.
        if car.get("payment_status") in (
            "REQUESTED",
            "PAYMENT_PENDING",
            "PAYMENT_HOLD",
            "WAITING_TO_CHARGE",
            "CHARGE_ERROR",
        ) and car.get("exit_arrival_time"):
            return

        seconds, minutes, parking, charging = calculate_charge(plate, sim_time)
        expected = round(parking + charging, 2)

        upsert_car(
            plate,
            exit_arrival_time=sim_time,
            exit_spot=spot_name,
            exit_zone=zone,
            exit_gate=gate,
            billable_seconds=seconds,
            billable_minutes=minutes,
            parking_cost=parking,
            charging_cost=charging,
            expected_amount=expected,
            payment_status="WAITING_TO_CHARGE",
            status="AT_EXIT"
        )

        total_multiplier = (
            ELECTRIC_TOTAL_MULTIPLIER
            if "electric" in str(car.get("car_type") or "").lower()
            else 1.0
        )
        audit(
            "system",
            "AT_EXIT",
            plate,
            (
                f"entry_time={car.get('entry_time')}; "
                f"parked_time={car.get('parked_time')}; "
                f"exit_arrival_time={sim_time}; "
                f"billable_seconds={seconds}; billable_minutes={minutes}; "
                f"parking={parking}; charging={charging}; "
                f"total={expected}; price_multiplier={total_multiplier}x"
            ),
            "PAYMENT_REQUIRED",
            sim_time
        )

        # Reference implementation sends the first charge almost immediately.
        def delayed_charge():
            time.sleep(0.1)
            latest = get_car(plate)
            if not latest or latest.get("payment_status") != "WAITING_TO_CHARGE":
                return
            try:
                request_payment_now(plate)
            except Exception as e:
                upsert_car(
                    plate,
                    payment_status="CHARGE_ERROR",
                    status="PAYMENT_HOLD"
                )
                upsert_alert(
                    f"PAYMENT:{plate}",
                    "HIGH",
                    "PAYMENT REQUEST FAILED",
                    str(e),
                    sim_time,
                    plate,
                    zone,
                    gate or ""
                )

        threading.Thread(target=delayed_charge, daemon=True).start()
        threading.Thread(
            target=payment_retry_watch,
            args=(plate,),
            daemon=True
        ).start()
        return

    if spot_type == "ExitSpot" and direction == "CarOut":
        car = get_car(plate) or {}
        paid = car.get("payment_status") == "PAID"

        if not paid:
            upsert_alert(
                f"UNPAID_EXIT:{plate}",
                "CRITICAL",
                "UNPAID VEHICLE DEPARTED",
                "Simulator reported ExitSpot CarOut without a valid PARKMIND payment authorization.",
                sim_time,
                plate,
                zone
            )

        total_stay_seconds = sim_seconds_between(car.get("entry_time"), sim_time)
        total_stay_minutes = (
            max(1, math.ceil(total_stay_seconds / 60))
            if total_stay_seconds > 0 else 0
        )

        upsert_car(
            plate,
            departure_time=sim_time,
            total_stay_seconds=total_stay_seconds,
            total_stay_minutes=total_stay_minutes,
            status="LEFT" if paid else "ESCAPED_UNPAID"
        )

        audit(
            "system",
            "DEPARTURE_TIME_CONFIRMED",
            plate,
            (
                f"entry_time={car.get('entry_time')}; "
                f"exit_arrival_time={car.get('exit_arrival_time')}; "
                f"departure_time={sim_time}; "
                f"total_stay_seconds={total_stay_seconds}; "
                f"billable_seconds={car.get('billable_seconds') or 0}; "
                f"expected={car.get('expected_amount') or 0}; "
                f"paid={car.get('actual_paid') or 0}"
            ),
            "OK" if paid else "UNPAID_ESCAPE_RECORDED",
            sim_time
        )

        gate = (
            find_active_exit_gate(plate)
            or car.get("exit_gate")
            or gate_for_sensor(spot_name, "exit")
        )

        if gate:
            if paid:
                remember_lane_mapping(
                    spot_name,
                    "exit",
                    gate,
                    confidence="CONFIRMED",
                    source="ExitSpot CarOut after valid payment"
                )
                resolve_alert(f"EXIT_MAP:{spot_name}", sim_time)
                resolve_alert(f"EXIT_DISCOVERY:{plate}", sim_time)

            with state_lock:
                if gate in exit_active and exit_active[gate].get("plate") == plate:
                    exit_active.pop(gate, None)

                # Remove any stale paid-release queue copy of this plate.
                if plate in paid_exit_queues.get(gate, []):
                    paid_exit_queues[gate] = [
                        p for p in paid_exit_queues[gate] if p != plate
                    ]

        # Physical queue progression is driven by REAL ExitSpot CarOut.
        remove_from_physical_exit_queue(spot_name, plate)
        set_state(f"exit_tried:{plate}", "")

        continue_physical_exit_queue(spot_name)

        # Only close the gate when no active release is using it.
        if gate:
            with state_lock:
                gate_busy = gate in exit_active
            if not gate_busy:
                try:
                    auto_restore_gate_api_baseline(gate, "system", sim_time)
                except Exception:
                    pass

        # A real departure frees exit approach capacity for another parked car.
        dispatch_exit_requests()
        return


def handle_payment(data, sim_time):
    plate = str(data.get("CarPlateNumber") or "").strip()
    received = float(data.get("Amount") or 0)

    ok, factors = verify_payment(plate, received)

    if not ok:
        car = get_car(plate) or {}
        upsert_car(
            plate,
            actual_paid=received,
            payment_status="INVALID",
            status="PAYMENT_HOLD",
            decision=factors
        )
        upsert_alert(
            f"PAYMENT:{plate}",
            "HIGH",
            "PAYMENT NOT ACCEPTED",
            factors,
            sim_time,
            plate,
            car.get("exit_zone") or "",
            car.get("exit_gate") or ""
        )
        audit(
            "system",
            "PAYMENT_REJECTED",
            plate,
            f"received={received}; {factors}",
            "GATE_REMAINS_CLOSED",
            sim_time
        )
        return

    car = get_car(plate) or {}

    upsert_car(
        plate,
        actual_paid=received,
        payment_time=sim_time,
        payment_status="PAID",
        status="PAID_WAITING_GATE",
        decision=factors
    )

    resolve_alert(f"PAYMENT:{plate}", sim_time)
    audit(
        "system",
        "PAYMENT_ACCEPTED",
        plate,
        f"received={received}; {factors}",
        "OK",
        sim_time
    )

    # The supplied reference releases a paid car only when it is physically
    # first in the exit queue. Same rule here, but independently per ExitSpot.
    queue_paid_vehicle_for_release(plate)




def handle_component_broken(data, sim_time):
    name = str(data.get("Name") or "").strip()
    kind = find_component_kind(name)
    if not name or not kind:
        return

    update_component_state(name, kind, broken=True, under=False, sim_time=sim_time)
    conn = db()
    conn.execute(
        """UPDATE components
           SET maintenance_required=1,
               maintenance_problem='BROKEN - corrective repair required'
           WHERE name=? AND kind=?""",
        (name, kind)
    )
    conn.commit()
    conn.close()
    row = component_row(name, kind)
    upsert_alert(
        f"BROKEN:{kind}:{name}", "CRITICAL", "COMPONENT BROKEN",
        f"Simulator reported {kind} {name} as broken. It is isolated from automatic use.",
        sim_time, zone=(row or {}).get("zone", ""), component=name
    )
    audit("system", "COMPONENT_BROKEN", name, kind, "ISOLATED", sim_time)

    if kind == "gate":
        # The signed component_broken webhook is authoritative now; do not let
        # a potentially lagging list endpoint overwrite it before lockdown.
        locked, gate_problems = gate_lockdown_status(refresh_live=False)
        if locked:
            hold_unreleased_gate_traffic(gate_problems, sim_time)

    # CO safety failover: if a fan breaks during an unsafe zone, try another healthy fan.
    if kind == "fan" and row:
        conn = db()
        z = conn.execute(
            "SELECT * FROM zones WHERE name=?",
            (row["zone"],)
        ).fetchone()
        conn.close()
        if z and danger_truthy(z["danger"]):
            handle_co(row["zone"], z["co_level"], z["danger"], sim_time)


def handle_component_fixed(data, sim_time):
    name = str(data.get("Name") or "").strip()
    kind = find_component_kind(name)
    if not name or not kind:
        return

    # RepairCost is supplied by the simulator's component_fixed webhook.
    # We do not estimate or invent it.
    repair_cost = _num(data.get("RepairCost"))

    conn = db()
    before = conn.execute(
        "SELECT broken FROM components WHERE name=? AND kind=?",
        (name, kind)
    ).fetchone()

    pending = conn.execute(
        """SELECT * FROM maintenance_actions
           WHERE component=?
             AND status IN('COMMAND_SENT','IN_PROGRESS','PENDING')
           ORDER BY id DESC LIMIT 1""",
        (name,)
    ).fetchone()

    conn.execute(
        """UPDATE components SET
           broken=0,
           under_maintenance=0,
           maintenance_required=0,
           maintenance_problem=NULL,
           cycles=0,
           runtime_seconds=0,
           on_since=NULL,
           api_life_pct=NULL,
           api_life_source=NULL,
           api_usage_current=NULL,
           api_usage_limit=NULL,
           api_runtime_hours=NULL,
           api_runtime_limit_hours=NULL,
           last_alarm_time=NULL,
           last_event_time=?
           WHERE name=? AND kind=?""",
        (sim_time, name, kind)
    )

    if pending:
        duration = sim_seconds_between(pending["simulator_time"], sim_time)
        conn.execute(
            """UPDATE maintenance_actions SET
               completed_simulator_time=?,
               repair_duration_seconds=?,
               repair_cost=?,
               cost_source=?,
               status='COMPLETED',
               result='COMPONENT_FIXED_WEBHOOK'
               WHERE id=?""",
            (
                sim_time,
                duration,
                repair_cost,
                "Simulator component_fixed.RepairCost" if repair_cost is not None else "Simulator did not provide RepairCost",
                pending["id"]
            )
        )
    else:
        # Component may have been repaired outside PARKMIND. Still preserve the
        # simulator-confirmed cost and state change.
        conn.execute(
            """INSERT INTO maintenance_actions(
               created_at,simulator_time,actor,component,kind,reason,action,result,
               repair_type,status,completed_simulator_time,repair_duration_seconds,
               repair_cost,cost_source
               ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                sim_time,
                "simulator/external",
                name,
                kind,
                "Component fixed event received without a PARKMIND pending repair command",
                "REPAIR",
                "COMPONENT_FIXED_WEBHOOK",
                "CORRECTIVE" if before and before["broken"] else "EXTERNAL",
                "COMPLETED",
                sim_time,
                0,
                repair_cost,
                "Simulator component_fixed.RepairCost" if repair_cost is not None else "Simulator did not provide RepairCost"
            )
        )

    conn.commit()
    conn.close()

    resolve_alert(f"BROKEN:{kind}:{name}", sim_time)
    resolve_alert(f"MAINT_DUE:{name}", sim_time)
    resolve_alert(f"MAINT_DUE:{kind}:{name}", sim_time)
    audit(
        "system", "COMPONENT_FIXED", name,
        f"{kind}; RepairCost={repair_cost if repair_cost is not None else 'not supplied'}",
        "OK", sim_time
    )

    if kind == "gate":
        # One repaired gate is not enough if another barrier is still broken
        # or under maintenance. resume_after_gate_lockdown() checks them ALL.
        resume_after_gate_lockdown(sim_time)


def handle_penalty(data, sim_time):
    reason = str(data.get("Reason") or "")
    fine = float(data.get("FineAmount") or 0)
    plate = str(
        data.get("CarPlateNumber")
        or data.get("PlateNumber")
        or data.get("CarPlate")
        or ""
    )
    conn = db()
    conn.execute(
        """INSERT INTO penalties(simulator_time,reason,fine_amount,plate,payload)
           VALUES(?,?,?,?,?)""",
        (sim_time, reason, fine, plate, json.dumps(data))
    )
    conn.commit()
    conn.close()
    upsert_alert(
        f"PENALTY:{data.get('EventId') or data.get('SequenceId') or time.time()}",
        "HIGH", "SIMULATOR PENALTY", reason,
        sim_time, plate
    )
    audit("system", "PENALTY", plate, f"{reason}; fine={fine}", "RECORDED", sim_time)


def handle_event(data):
    event_class = data.get("EventClass")
    sim_time = data.get("ServerDateTime")
    if sim_time:
        set_state("last_simulator_time", sim_time)

    try:
        if event_class == "gate_action":
            handle_gate_action(data, sim_time)
        elif event_class == "car_spot_action":
            handle_car_event(data, sim_time)
        elif event_class == "payment_made":
            handle_payment(data, sim_time)
        elif event_class == "component_broken":
            handle_component_broken(data, sim_time)
        elif event_class == "component_fixed":
            handle_component_fixed(data, sim_time)
        elif event_class == "carbon_monoxide_event":
            handle_co(
                str(data.get("ZoneName") or ""),
                data.get("CarbonMonoxideLevel"),
                data.get("DangerLevel"),
                sim_time
            )
        elif event_class == "penalty":
            handle_penalty(data, sim_time)
        elif event_class == "test_webhook":
            audit("system", "TEST_WEBHOOK", "", "Signed simulator test received", "OK", sim_time)

        # Energy + preventive maintenance use the same simulator timestamp.
        if sim_time:
            apply_light_policy(sim_time)
            evaluate_preventive_maintenance(sim_time)

    except Exception as e:
        audit("system", "EVENT_HANDLER_ERROR", str(event_class), str(e), "ERROR", sim_time)
        upsert_alert(
            f"EVENT_ERROR:{event_class}",
            "HIGH", "EVENT PROCESSING ERROR",
            str(e), sim_time
        )


@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(silent=True) or {}
    result = verify_webhook_signature(data)

    if result is True:
        status = "verified"
    elif result is None:
        status = "unsigned"
    else:
        status = "invalid"

    is_new = save_event(data, status)
    if not is_new:
        return jsonify({"status": "duplicate_ignored"}), 200

    # Level 2 rule: log unsigned/invalid, but DO NOT ACT on them.
    if result is not True:
        audit(
            "system", "WEBHOOK_SECURITY_BLOCK", data.get("EventClass") or "",
            f"signature={status}", "IGNORED", data.get("ServerDateTime")
        )
        return jsonify({"status": f"{status}_logged_not_processed"}), 200

    check_sequence(data.get("SequenceId"))
    threading.Thread(target=handle_event, args=(data,), daemon=True).start()
    return jsonify({"status": "received_verified"}), 200


# ============================================================
# AUTHENTICATION + RBAC
# ============================================================
def role_home(role):
    return {
        "Admin": "/admin/",
        "Operator": "/operator/",
        "Maintenance": "/maintenance/",
    }.get(role, "/login")


def require_roles(*roles):
    return bool(session.get("user")) and session.get("role") in roles


@app.route("/", methods=["GET"])
def root():
    if not session.get("user"):
        return redirect(url_for("login"))
    return redirect(role_home(session.get("role")))


@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        username = (request.form.get("username") or "").strip().lower()
        password = request.form.get("password") or ""
        user = USERS.get(username)
        success = bool(user and user["password"] == password)
        record_login(username, success)

        if success:
            session.clear()
            session["user"] = username
            session["role"] = user["role"]
            audit(username, "LOGIN_SUCCESS", username, user["role"], "OK")
            return redirect(role_home(user["role"]))

        audit(username or "unknown", "LOGIN_FAILED", username, "", "DENIED")
        error = "Invalid name or password"

    return render_template_string(LOGIN_HTML, error=error)


@app.route("/logout")
def logout():
    actor = session.get("user") or "unknown"
    audit(actor, "LOGOUT", actor, "", "OK")
    session.clear()
    return redirect(url_for("login"))


# ============================================================
# ROLE-SAFE CONTROL ENDPOINTS
# ============================================================
def redirect_back():
    return redirect(role_home(session.get("role")))


@app.route("/control/gate/<name>/<action>", methods=["POST"])
def control_gate(name, action):
    if not require_roles("Operator", "Admin"):
        return ("Forbidden", 403)
    if action not in ("open", "close"):
        return ("Bad action", 400)

    # /list-barriers does NOT expose entry/exit roles, so PARKMIND does not
    # invent a paid-only label for a named barrier.
    #
    # Payment protection uses only real vehicle state already tied to this
    # gate by the controller. Automatic exit release still happens only after
    # a verified PAID state.
    if action == "open":
        locked, gate_problems = gate_lockdown_status(refresh_live=True)
        if locked:
            mark_gate_lockdown(gate_problems, simulator_now())
            audit(
                session["user"],
                "MANUAL_GATE_OPEN_BLOCKED_GLOBAL_LOCKDOWN",
                name,
                gate_lockdown_reason(gate_problems),
                "DENIED",
                simulator_now()
            )
            return redirect_back()

        # API does not expose gate roles. For safety, if ANY vehicle is
        # physically waiting at an ExitSpot without PAID status, manual OPEN is
        # blocked for unknown barriers. Automatic entry handling is unaffected.
        unpaid_exit_plate = None
        with state_lock:
            physical_fronts = [
                q[0] for q in physical_exit_queues.values() if q
            ]
        for candidate_plate in physical_fronts:
            candidate = get_car(candidate_plate) or {}
            if candidate.get("payment_status") != "PAID":
                unpaid_exit_plate = candidate_plate
                break

        if unpaid_exit_plate:
            audit(
                session["user"], "MANUAL_GATE_OPEN_BLOCKED_UNPAID_EXIT", name,
                f"Unpaid vehicle {unpaid_exit_plate} is physically waiting at an ExitSpot.",
                "DENIED", simulator_now()
            )
            return redirect_back()

        blocker = gate_has_unpaid_blocker(name)
        if blocker:
            audit(
                session["user"], "GATE_OPEN_BLOCKED", name,
                f"Unpaid vehicle physically associated with this gate: {blocker}",
                "DENIED", simulator_now()
            )
            upsert_alert(
                f"MANUAL_GATE_INTERLOCK:{name}",
                "HIGH", "GATE OPEN BLOCKED",
                f"Manual open denied because {blocker} is still unpaid at its exit flow.",
                simulator_now(), blocker, component=name
            )
            return redirect_back()

    try:
        component_action("gate", name, action, session["user"], simulator_now())
    except Exception as e:
        upsert_alert(
            f"MANUAL_GATE:{name}", "HIGH", "GATE CONTROL FAILED",
            str(e), simulator_now(), component=name
        )
    return redirect_back()


@app.route("/control/car/<plate>/exit", methods=["POST"])
def manual_exit_request(plate):
    if not require_roles("Operator", "Admin"):
        return ("Forbidden", 403)
    car = get_car(plate)
    if car and car.get("status") == "PARKED":
        with state_lock:
            if plate not in exit_request_queue:
                exit_request_queue.append(plate)
        audit(session["user"], "MANUAL_EXIT_REQUEST", plate, "", "OK", simulator_now())
        dispatch_exit_requests()
    return redirect_back()


@app.route("/control/repair/<kind>/<name>", methods=["POST"])
def repair_component(kind, name):
    if not require_roles("Maintenance", "Admin"):
        return ("Forbidden", 403)

    if kind not in ("spot", "gate", "fan"):
        audit(
            session.get("user") or "unknown",
            "REPAIR_BLOCKED", name,
            f"No documented repair endpoint for kind={kind}",
            "DENIED", simulator_now()
        )
        return redirect_back()

    row = component_row(name, kind)
    if not row:
        return ("Unknown component", 404)

    safe, safety_reason = safe_for_maintenance(row)
    if not safe:
        upsert_alert(
            f"REPAIR_BLOCKED:{kind}:{name}",
            "HIGH", "REPAIR WAITING FOR SAFE WINDOW",
            safety_reason,
            simulator_now(),
            zone=row.get("zone") or "",
            component=name
        )
        audit(
            session["user"], "REPAIR_BLOCKED", name,
            safety_reason, "DENIED", simulator_now()
        )
        return redirect_back()

    conn = db()
    existing = conn.execute(
        """SELECT id FROM maintenance_actions
           WHERE component=?
             AND status IN('COMMAND_SENT','IN_PROGRESS','PENDING')
           ORDER BY id DESC LIMIT 1""",
        (name,)
    ).fetchone()
    conn.close()
    if existing:
        return redirect_back()

    repair_type = (
        "CORRECTIVE" if int(row.get("broken") or 0)
        else "PREVENTIVE" if int(row.get("maintenance_required") or 0)
        else "MANUAL"
    )
    reason = row.get("maintenance_problem") or (
        "Broken component" if int(row.get("broken") or 0)
        else "Authorized maintenance"
    )
    started = simulator_now() or None

    conn = db()
    cur = conn.execute(
        """INSERT INTO maintenance_actions(
           created_at,simulator_time,actor,component,kind,reason,action,result,
           repair_type,status,cost_source
           ) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
        (
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            started,
            session["user"],
            name,
            kind,
            reason,
            "REPAIR",
            "PREPARING_COMMAND",
            repair_type,
            "PENDING",
            "Awaiting simulator component_fixed.RepairCost"
        )
    )
    action_id = cur.lastrowid
    conn.commit()
    conn.close()

    try:
        component_action(kind, name, "repair", session["user"], started)
        conn = db()
        conn.execute(
            """UPDATE maintenance_actions
               SET status='COMMAND_SENT',
                   result='REPAIR_COMMAND_ACCEPTED'
               WHERE id=?""",
            (action_id,)
        )
        conn.commit()
        conn.close()
        resolve_alert(f"REPAIR_BLOCKED:{kind}:{name}", started)
    except Exception as e:
        conn = db()
        conn.execute(
            """UPDATE maintenance_actions
               SET status='FAILED',result=?
               WHERE id=?""",
            (f"ERROR: {e}", action_id)
        )
        conn.commit()
        conn.close()
        audit(session["user"], "REPAIR_COMMAND_FAILED", name, str(e), "ERROR", started)

    return redirect_back()


@app.route("/control/refresh-maintenance", methods=["POST"])
def refresh_maintenance_control():
    if not require_roles("Maintenance", "Admin"):
        return ("Forbidden", 403)
    try:
        refresh_maintenance_snapshot(f"maintenance-refresh:{session['user']}")
    except Exception as e:
        audit(session["user"], "MAINTENANCE_REFRESH_FAILED", "", str(e), "ERROR")
    return redirect_back()


@app.route("/control/fan/<name>/<action>", methods=["POST"])
def control_fan(name, action):
    if not require_roles("Maintenance", "Admin"):
        return ("Forbidden", 403)
    if action not in ("on", "off"):
        return ("Bad action", 400)
    try:
        component_action("fan", name, action, session["user"], simulator_now())
    except Exception:
        pass
    return redirect_back()


@app.route("/control/light/<name>/<action>", methods=["POST"])
def control_light(name, action):
    if not require_roles("Maintenance", "Admin"):
        return ("Forbidden", 403)
    if action not in ("on", "off"):
        return ("Bad action", 400)
    try:
        component_action("light", name, action, session["user"], simulator_now())
    except Exception:
        pass
    return redirect_back()


@app.route("/control/sync", methods=["POST"])
def control_sync():
    if not require_roles("Maintenance", "Admin"):
        return ("Forbidden", 403)
    safe_sync_topology(f"manual:{session['user']}")
    return redirect_back()


# ============================================================
# REGISTER THE THREE SEPARATE DASHBOARD FILES
# ============================================================
from operator_dashboard import operator_bp
from admin_dashboard import admin_bp
from maintenance_dashboard import maintenance_bp

app.register_blueprint(operator_bp)
app.register_blueprint(admin_bp)
app.register_blueprint(maintenance_bp)


if __name__ == "__main__":
    init_db()
    try:
        sim_login()
        sync_topology("startup")
        released = cleanup_stale_reservations()

        locked, gate_problems = gate_lockdown_status(refresh_live=True)
        if locked:
            hold_unreleased_gate_traffic(gate_problems, simulator_now())
        else:
            set_state("gate_lockdown", "0")
        print(
            "[PARKMIND] Level 2 topology loaded from simulator APIs. "
            f"Stale reservations released={released}."
        )
    except Exception as e:
        print("[PARKMIND] Simulator not ready:", e)
        print("[PARKMIND] Start/load Level 2, then use Admin/Maintenance Sync once.")

    print("\n====================================================")
    print(" PARKMIND LEVEL 2")
    print(" Login:       http://127.0.0.1:8000/login")
    print(" Admin:       admin / admin")
    print(" Operator:    operator / operator")
    print(" Maintenance: maintenance / maintenance")
    print(" Webhook:     http://127.0.0.1:8000/webhook")
    print(" Level 2: ONLY VALID SIGNED WEBHOOKS ARE PROCESSED")
    print(" Flow: Entry -> least-loaded API zone -> compatible bay -> API ExitSpot -> pay -> leave")
    counts = topology_counts()
    print(
        " Topology: "
        f"{counts['park']} Park bays | {counts['entry']} EntrySpot | "
        f"{counts['exit']} ExitSpot | {counts['zones']} zones | "
        f"{counts['gates']} barriers (all discovered from API)"
    )
    print(" Gate roles: NOT exposed by API; no barrier is labelled entry/exit by assumption")
    print(" Gate safety: broken/maintenance gates are never operated")
    print(" GLOBAL LOCKDOWN: ANY broken/maintenance barrier blocks ALL new entry + exit until repaired")
    print(" Gate interlock: car movement is released ONLY after /list-barriers confirms state=Open")
    print(" Gate hold-open: entry/exit gate stays open until the real CarOut webhook confirms crossing")
    print(" Zone flow: target-zone barriers come from API zoneParent and stay open until real Park CarIn")
    print(" Anti-pile: entry gate waits briefly before returning to its startup API state")
    print(" Payment interlock: unpaid vehicles NEVER receive goto/leavepark")
    print(" Billing: simulator timestamps; Electric total = 2x base parking price")
    print(" Gate topology: automatic cleanup restores first /list-barriers API state")
    print(" Lane mapping: only physical simulator crossings are treated as confirmed")
    print("====================================================\n")

    app.run(host="0.0.0.0", port=WEB_PORT, threaded=True)
