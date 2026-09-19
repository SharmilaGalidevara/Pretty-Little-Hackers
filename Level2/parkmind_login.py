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

# Preventive-maintenance policy thresholds.
# These are PARKMIND policy settings, not fabricated simulator readings.
MAINT_CYCLES = {
    "spot": int(os.getenv("PARKMIND_SPOT_MAINT_CYCLES", "20")),
    "gate": int(os.getenv("PARKMIND_GATE_MAINT_CYCLES", "30")),
    "fan": int(os.getenv("PARKMIND_FAN_MAINT_CYCLES", "20")),
    "light": int(os.getenv("PARKMIND_LIGHT_MAINT_CYCLES", "50")),
}
MAINT_RUNTIME_SECONDS = {
    "fan": int(os.getenv("PARKMIND_FAN_MAINT_RUNTIME", "1800")),
    "light": int(os.getenv("PARKMIND_LIGHT_MAINT_RUNTIME", "3600")),
}

# Configured tariff used to calculate the amount sent to the simulator.
# Durations themselves always come from simulator timestamps.
PARKING_RATE_PER_MIN = float(os.getenv("PARKMIND_PARKING_RATE", "1"))
EV_CHARGING_RATE_PER_MIN = float(os.getenv("PARKMIND_EV_CHARGING_RATE", "1"))

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
paid_exit_queues = defaultdict(list)      # gate -> [plate]
exit_active = {}                          # gate -> {plate, sent}
exit_request_queue = []                   # parked cars waiting to approach exit
scheduled_exit_plates = set()

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
        result TEXT
    );
    """)
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

    broken = 1 if item.get("broken", False) else 0
    under = 1 if item.get("isUnderMaintenance", False) else 0

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
            runtime_seconds,on_since,last_event_time,raw_json
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(name,kind) DO UPDATE SET
            zone=excluded.zone,
            state=excluded.state,
            broken=excluded.broken,
            under_maintenance=excluded.under_maintenance,
            last_event_time=excluded.last_event_time,
            raw_json=excluded.raw_json""",
        (
            name, kind, zone, str(state), broken, under,
            cycles, runtime, on_since,
            sim_time or simulator_now() or None,
            json.dumps(item)
        )
    )
    conn.commit()
    conn.close()


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
            for item in loaded[kind]:
                upsert_component(kind, item)
        except Exception as e:
            loaded[kind] = []
            audit("system", "TOPOLOGY_SYNC_ERROR", kind, str(e), "ERROR")

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

    infer_gate_roles()
    set_state("topology_loaded", "1")
    audit("system", "TOPOLOGY_SYNC", reason,
          ", ".join(f"{k}={len(v)}" for k, v in loaded.items()))


def infer_gate_roles():
    conn = db()
    gates = [dict(r) for r in conn.execute(
        "SELECT name,zone,raw_json FROM components WHERE kind='gate'"
    ).fetchall()]
    spots = [dict(r) for r in conn.execute(
        "SELECT name,zone,raw_json FROM components WHERE kind='spot'"
    ).fetchall()]

    zone_purposes = defaultdict(set)
    for row in spots:
        try:
            raw = json.loads(row["raw_json"] or "{}")
        except Exception:
            raw = {}
        purpose = str(raw.get("purpose") or "")
        if purpose:
            zone_purposes[row["zone"]].add(purpose)

    for g in gates:
        name_lower = g["name"].lower()
        role = "unknown"
        source = "unresolved"

        if any(x in name_lower for x in ("entry", "entrance", "inbound", "gatein")):
            role, source = "entry", "name"
        elif any(x in name_lower for x in ("exit", "outbound", "gateout")):
            role, source = "exit", "name"
        else:
            same_zone = [x for x in gates if x["zone"] == g["zone"]]
            purposes = zone_purposes.get(g["zone"], set())
            if len(same_zone) == 1 and "EntrySpot" in purposes and "ExitSpot" not in purposes:
                role, source = "entry", "zone-purpose"
            elif len(same_zone) == 1 and "ExitSpot" in purposes and "EntrySpot" not in purposes:
                role, source = "exit", "zone-purpose"

        conn.execute(
            """INSERT INTO gate_roles(name,role,zone,source)
               VALUES(?,?,?,?)
               ON CONFLICT(name) DO UPDATE SET
                 role=excluded.role,zone=excluded.zone,source=excluded.source""",
            (g["name"], role, g["zone"], source)
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
    row = component_row(sensor_name, "spot")
    return row.get("zone", "") if row else ""


def gate_for_sensor(sensor_name, role):
    zone = sensor_zone(sensor_name)
    conn = db()

    # First use a confidently inferred gate role in the same zone.
    rows = conn.execute(
        """SELECT g.name
           FROM gate_roles g
           JOIN components c ON c.name=g.name AND c.kind='gate'
           WHERE g.role=? AND g.zone=?
             AND c.broken=0 AND c.under_maintenance=0""",
        (role, zone)
    ).fetchall()
    if len(rows) == 1:
        conn.close()
        return rows[0]["name"]

    # If the zone has exactly one healthy barrier, using it is not an invented mapping.
    rows = conn.execute(
        """SELECT name FROM components
           WHERE kind='gate' AND zone=? AND broken=0 AND under_maintenance=0""",
        (zone,)
    ).fetchall()
    if len(rows) == 1:
        conn.close()
        return rows[0]["name"]

    # Last safe fallback: exactly one gate of this role in the whole topology.
    rows = conn.execute(
        """SELECT g.name
           FROM gate_roles g
           JOIN components c ON c.name=g.name AND c.kind='gate'
           WHERE g.role=? AND c.broken=0 AND c.under_maintenance=0""",
        (role,)
    ).fetchall()
    conn.close()
    if len(rows) == 1:
        return rows[0]["name"]

    return None


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
        ("light", "repair"): f"/lights/{quote(name, safe='')}/repair",
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

    if action not in ("repair",) and int(row.get("broken") or 0):
        raise RuntimeError(f"{name} is broken")

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


def choose_spot(car_type):
    conn = db()
    rows = [dict(r) for r in conn.execute(
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

    zone_counts = {}
    all_spots = [dict(r) for r in conn.execute(
        "SELECT zone,state FROM components WHERE kind='spot'"
    ).fetchall()]
    conn.close()

    for r in all_spots:
        z = r["zone"] or ""
        item = zone_counts.setdefault(z, {"total": 0, "busy": 0})
        item["total"] += 1
        if r["state"] == "Occupied":
            item["busy"] += 1

    candidates = []
    for row in rows:
        try:
            raw = json.loads(row["raw_json"] or "{}")
        except Exception:
            raw = {}

        if str(raw.get("purpose") or "") != "Park":
            continue
        if not is_compatible(raw, car_type):
            continue

        zone = row["zone"] or ""
        z = zone_counts.get(zone, {"total": 1, "busy": 0})
        load = z["busy"] / max(1, z["total"])
        wear = int(row["cycles"] or 0) / max(1, MAINT_CYCLES["spot"])
        score = (1 - load) * 0.65 + max(0, 1 - wear) * 0.35
        candidates.append((score, row["name"], zone))

    if not candidates:
        return None, None
    candidates.sort(reverse=True)
    return candidates[0][1], candidates[0][2]


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
# MULTI-ENTRY CONTROL
# ============================================================
def process_entry_gate(gate):
    with state_lock:
        if gate in entry_active or not entry_queues[gate]:
            return
        item = entry_queues[gate].pop(0)
        entry_active[gate] = {
            "plate": item["plate"],
            "spot": item["spot"],
            "sent": False
        }

    try:
        gate_row = component_row(gate, "gate") or {}
        if str(gate_row.get("state") or "").lower() in ("open", "opening"):
            send_car(item["plate"], item["spot"])
            with state_lock:
                if gate in entry_active:
                    entry_active[gate]["sent"] = True
            upsert_car(item["plate"], status="TO_SPOT")
        else:
            component_action("gate", gate, "open", "system", simulator_now())
    except Exception as e:
        with state_lock:
            entry_active.pop(gate, None)
            entry_queues[gate].insert(0, item)
        upsert_alert(
            f"ENTRY_GATE:{gate}",
            "CRITICAL", "ENTRY GATE UNAVAILABLE",
            f"Could not open {gate}: {e}",
            simulator_now(), item["plate"], component=gate
        )


def enqueue_arrival(plate, car_type, planned, entry_spot, sim_time):
    zone = sensor_zone(entry_spot)
    spot, spot_zone = choose_spot(car_type)
    if not spot:
        upsert_car(plate, status="NO_SAFE_SPACE", decision="No compatible safe spot")
        upsert_alert(
            f"NO_SPACE:{plate}", "HIGH", "NO SAFE PARKING SPACE",
            "No compatible free healthy spot is available.",
            sim_time, plate, zone
        )
        try:
            send_car(plate, "leavepark")
        except Exception:
            pass
        return

    gate = gate_for_sensor(entry_spot, "entry")
    if not gate:
        reserve_spot(spot, plate, sim_time)
        upsert_car(
            plate, assigned_spot=spot, status="ENTRY_HOLD",
            decision="Entry gate topology unresolved"
        )
        upsert_alert(
            f"ENTRY_MAP:{entry_spot}", "CRITICAL", "ENTRY GATE MAPPING REQUIRED",
            f"PARKMIND cannot safely identify the barrier serving {entry_spot}.",
            sim_time, plate, zone
        )
        return

    reserve_spot(spot, plate, sim_time)
    upsert_car(
        plate,
        car_type=car_type,
        planned_minutes=planned,
        entry_time=sim_time,
        entry_spot=entry_spot,
        entry_zone=zone,
        assigned_spot=spot,
        status="WAITING_ENTRY",
        decision=f"Assigned {spot} in {spot_zone}"
    )

    with state_lock:
        entry_queues[gate].append({"plate": plate, "spot": spot})
    process_entry_gate(gate)


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


def dispatch_exit_requests():
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
        try:
            send_car(plate, "exit")
            upsert_car(plate, status="TO_EXIT")
            active += 1
        except Exception as e:
            upsert_alert(
                f"EXIT_ROUTE:{plate}", "HIGH", "EXIT ROUTE FAILURE",
                str(e), simulator_now(), plate
            )
            break


def calculate_charge(plate, exit_sim_time):
    car = get_car(plate)
    if not car or not car.get("parked_time"):
        return 0, 0, 0, 0

    seconds = sim_seconds_between(car["parked_time"], exit_sim_time)
    minutes = max(1, math.ceil(seconds / 60))
    parking = round(minutes * PARKING_RATE_PER_MIN, 2)
    charging = 0.0
    if "electric" in str(car.get("car_type") or "").lower():
        charging = round(minutes * EV_CHARGING_RATE_PER_MIN, 2)
    return seconds, minutes, parking, charging


def gate_has_unpaid_blocker(gate):
    conn = db()
    row = conn.execute(
        """SELECT plate FROM cars
           WHERE exit_gate=?
             AND exit_arrival_time IS NOT NULL
             AND departure_time IS NULL
             AND payment_status!='PAID'
             AND status IN('AT_EXIT','PAYMENT_PENDING','PAYMENT_HOLD','UNREGISTERED_EXIT')
           LIMIT 1""",
        (gate,)
    ).fetchone()
    conn.close()
    return row["plate"] if row else None


def process_paid_exit_gate(gate):
    with state_lock:
        if gate in exit_active or not paid_exit_queues[gate]:
            return

    blocker = gate_has_unpaid_blocker(gate)
    if blocker:
        upsert_alert(
            f"EXIT_BLOCKED:{gate}", "CRITICAL", "EXIT LANE BLOCKED",
            f"{blocker} has not completed valid payment. Gate {gate} remains closed.",
            simulator_now(), blocker, component=gate
        )
        return
    resolve_alert(f"EXIT_BLOCKED:{gate}", simulator_now())

    with state_lock:
        if not paid_exit_queues[gate]:
            return
        plate = paid_exit_queues[gate].pop(0)
        exit_active[gate] = {"plate": plate, "sent": False}

    upsert_car(plate, status="PAID_WAITING_GATE")
    try:
        gate_row = component_row(gate, "gate") or {}
        if str(gate_row.get("state") or "").lower() in ("open", "opening"):
            # The barrier is already open from the previous authorised car.
            # Send ONLY this already-paid vehicle; unpaid cars never enter this queue.
            send_car(plate, "leavepark")
            with state_lock:
                if gate in exit_active:
                    exit_active[gate]["sent"] = True
            audit(
                "system", "PAID_EXIT_FAST_PATH", plate,
                f"{gate} already open; released paid vehicle only",
                "OK", simulator_now()
            )
        else:
            component_action("gate", gate, "open", "system", simulator_now())
    except Exception as e:
        with state_lock:
            exit_active.pop(gate, None)
            paid_exit_queues[gate].insert(0, plate)
        upsert_alert(
            f"EXIT_GATE:{gate}", "CRITICAL", "EXIT GATE FAILURE",
            f"Paid vehicle {plate} cannot be released: {e}",
            simulator_now(), plate, component=gate
        )


def verify_payment(plate, amount):
    car = get_car(plate)
    if not car or not car.get("entry_time"):
        return False, "UNTRACKED_VEHICLE"
    if car.get("status") not in ("AT_EXIT", "PAYMENT_PENDING", "PAYMENT_HOLD"):
        return False, f"BAD_STATUS:{car.get('status')}"
    if car.get("payment_status") == "PAID":
        return False, "DOUBLE_PAYMENT"
    expected = float(car.get("expected_amount") or 0)
    if abs(float(amount) - expected) > 0.001:
        if float(amount) < expected:
            return False, f"INSUFFICIENT_FUNDS expected={expected} received={amount}"
        return False, f"AMOUNT_MISMATCH expected={expected} received={amount}"
    return True, "OK"


# ============================================================
# CO SAFETY + ENERGY POLICY
# ============================================================
def danger_truthy(value):
    return str(value).strip().lower() in (
        "1", "true", "danger", "high", "critical", "yes", "unsafe"
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
    if int(row.get("broken") or 0):
        return 0
    if int(row.get("under_maintenance") or 0):
        return 20

    kind = row["kind"]
    cycle_limit = MAINT_CYCLES.get(kind)
    runtime_limit = MAINT_RUNTIME_SECONDS.get(kind)
    wear = 0.0
    if cycle_limit:
        wear = max(wear, int(row.get("cycles") or 0) / max(1, cycle_limit))
    if runtime_limit:
        wear = max(wear, int(row.get("runtime_seconds") or 0) / max(1, runtime_limit))
    return max(0, round(100 * (1 - min(wear, 1.0))))


def safe_for_maintenance(row):
    kind = row["kind"]
    if kind == "spot":
        conn = db()
        reserved = conn.execute(
            "SELECT 1 FROM reservations WHERE spot=?",
            (row["name"],)
        ).fetchone()
        conn.close()
        return row["state"] != "Occupied" and not reserved
    if kind == "gate":
        with state_lock:
            busy = row["name"] in entry_active or row["name"] in exit_active
        return str(row["state"]).lower() == "closed" and not busy
    if kind == "fan":
        conn = db()
        z = conn.execute(
            "SELECT danger FROM zones WHERE name=?",
            (row["zone"],)
        ).fetchone()
        conn.close()
        return str(row["state"]).lower() != "on" and not (z and danger_truthy(z["danger"]))
    if kind == "light":
        return str(row["state"]).lower() != "on"
    return False


def evaluate_preventive_maintenance(sim_time):
    conn = db()
    rows = [dict(r) for r in conn.execute(
        """SELECT * FROM components
           WHERE kind IN('spot','gate','fan','light')
             AND broken=0 AND under_maintenance=0"""
    ).fetchall()]
    conn.close()

    for row in rows:
        health = component_health(row)
        key = f"MAINT_DUE:{row['kind']}:{row['name']}"
        if health > 20:
            resolve_alert(key, sim_time)
            continue

        reason = (
            f"Preventive maintenance due: health={health}%, "
            f"cycles={row['cycles']}, runtime={fmt_duration(row['runtime_seconds'])}."
        )
        upsert_alert(
            key, "HIGH", "PREVENTIVE MAINTENANCE DUE",
            reason, sim_time, zone=row["zone"], component=row["name"]
        )

        # Efficient repair: only attempt automatically in a safe idle window.
        if safe_for_maintenance(row):
            try:
                component_action(row["kind"], row["name"], "repair", "system", sim_time)
                conn = db()
                conn.execute(
                    """INSERT INTO maintenance_actions(
                       created_at,simulator_time,actor,component,kind,reason,action,result
                       ) VALUES(?,?,?,?,?,?,?,?)""",
                    (
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        sim_time, "system", row["name"], row["kind"],
                        reason, "PREVENTIVE_REPAIR", "COMMAND_SENT"
                    )
                )
                conn.commit()
                conn.close()
            except Exception as e:
                audit("system", "PREVENTIVE_REPAIR_FAILED", row["name"], str(e), "ERROR", sim_time)


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

    if incoming and action in ("Opening", "Open") and not incoming["sent"]:
        try:
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

    if outgoing and action in ("Opening", "Open") and not outgoing["sent"]:
        try:
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
        car = get_car(plate)
        gate = gate_for_sensor(spot_name, "entry")
        if gate:
            with state_lock:
                if gate in entry_active and entry_active[gate]["plate"] == plate:
                    entry_active.pop(gate, None)
                has_next = bool(entry_queues[gate])
            if has_next:
                process_entry_gate(gate)
            else:
                try:
                    component_action("gate", gate, "close", "system", sim_time)
                except Exception:
                    pass
        if car:
            upsert_car(plate, status="TO_SPOT")
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
        else:
            update_component_state(spot_name, "spot", "Free", sim_time=sim_time)
        return

    if spot_type == "ExitSpot" and direction == "CarIn":
        car = get_car(plate)
        gate = gate_for_sensor(spot_name, "exit")

        # Real-world exception: car appears at exit without trusted entry history.
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
                "CRITICAL", "UNREGISTERED VEHICLE AT EXIT",
                "No trusted entry timestamp exists. Gate remains closed; security/operator verification is required.",
                sim_time, plate, zone, gate or ""
            )
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

        if not gate:
            upsert_alert(
                f"EXIT_MAP:{spot_name}", "CRITICAL", "EXIT GATE MAPPING REQUIRED",
                f"PARKMIND cannot safely identify the gate serving exit sensor {spot_name}.",
                sim_time, plate, zone
            )

        def request_charge():
            time.sleep(1.2)
            latest = get_car(plate)
            if not latest or latest.get("payment_status") != "WAITING_TO_CHARGE":
                return
            try:
                upsert_car(plate, payment_status="REQUESTED", status="PAYMENT_PENDING")
                charge_car(plate, parking, charging)
            except Exception as e:
                upsert_car(plate, payment_status="CHARGE_ERROR", status="PAYMENT_HOLD")
                upsert_alert(
                    f"PAYMENT:{plate}", "CRITICAL", "PAYMENT REQUEST FAILED",
                    str(e), sim_time, plate, zone
                )

        threading.Thread(target=request_charge, daemon=True).start()
        return

    if spot_type == "ExitSpot" and direction == "CarOut":
        car = get_car(plate) or {}
        paid = car.get("payment_status") == "PAID"

        if not paid:
            upsert_alert(
                f"UNPAID_EXIT:{plate}", "CRITICAL", "UNPAID VEHICLE DEPARTED",
                "Simulator reported ExitSpot CarOut without a valid PARKMIND payment authorization.",
                sim_time, plate, zone
            )

        upsert_car(
            plate,
            departure_time=sim_time,
            status="LEFT" if paid else "ESCAPED_UNPAID"
        )

        gate = car.get("exit_gate") or gate_for_sensor(spot_name, "exit")
        if gate:
            with state_lock:
                if gate in exit_active and exit_active[gate]["plate"] == plate:
                    exit_active.pop(gate, None)

            # If another PAID vehicle is waiting and no unpaid blocker exists,
            # keep throughput moving. Otherwise close the barrier.
            if paid_exit_queues[gate] and not gate_has_unpaid_blocker(gate):
                process_paid_exit_gate(gate)
            else:
                try:
                    component_action("gate", gate, "close", "system", sim_time)
                except Exception:
                    pass

        dispatch_exit_requests()
        return


def handle_payment(data, sim_time):
    plate = str(data.get("CarPlateNumber") or "").strip()
    amount = float(data.get("Amount") or 0)
    ok, reason = verify_payment(plate, amount)

    if not ok:
        car = get_car(plate) or {}
        upsert_car(
            plate,
            actual_paid=amount,
            payment_status="INVALID",
            status="PAYMENT_HOLD"
        )
        upsert_alert(
            f"PAYMENT:{plate}", "CRITICAL", "PAYMENT NOT ACCEPTED",
            reason, sim_time, plate,
            car.get("exit_zone") or "", car.get("exit_gate") or ""
        )
        audit("system", "PAYMENT_REJECTED", plate, reason, "BLOCKED", sim_time)
        return

    car = get_car(plate)
    upsert_car(
        plate,
        actual_paid=amount,
        payment_time=sim_time,
        payment_status="PAID",
        status="PAID_WAITING_GATE"
    )
    resolve_alert(f"PAYMENT:{plate}", sim_time)
    audit("system", "PAYMENT_ACCEPTED", plate, f"amount={amount}", "OK", sim_time)

    gate = car.get("exit_gate")
    if not gate:
        upsert_alert(
            f"EXIT_NO_GATE:{plate}", "CRITICAL", "PAID CAR HAS NO EXIT GATE",
            "Payment is valid but exit topology is unresolved; vehicle remains safely held.",
            sim_time, plate, car.get("exit_zone") or ""
        )
        return

    with state_lock:
        if plate not in paid_exit_queues[gate]:
            paid_exit_queues[gate].append(plate)
    process_paid_exit_gate(gate)


def handle_component_broken(data, sim_time):
    name = str(data.get("Name") or "").strip()
    kind = find_component_kind(name)
    if not name or not kind:
        return

    update_component_state(name, kind, broken=True, under=False, sim_time=sim_time)
    row = component_row(name, kind)
    upsert_alert(
        f"BROKEN:{kind}:{name}", "CRITICAL", "COMPONENT BROKEN",
        f"Simulator reported {kind} {name} as broken. It is isolated from automatic use.",
        sim_time, zone=(row or {}).get("zone", ""), component=name
    )
    audit("system", "COMPONENT_BROKEN", name, kind, "ISOLATED", sim_time)

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

    conn = db()
    conn.execute(
        """UPDATE components SET broken=0,under_maintenance=0,
           cycles=0,runtime_seconds=0,on_since=NULL,last_event_time=?
           WHERE name=? AND kind=?""",
        (sim_time, name, kind)
    )
    conn.commit()
    conn.close()
    resolve_alert(f"BROKEN:{kind}:{name}", sim_time)
    resolve_alert(f"MAINT_DUE:{kind}:{name}", sim_time)
    audit("system", "COMPONENT_FIXED", name, kind, "OK", sim_time)


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

    # Payment interlock: nobody can use the dashboard to bypass payment.
    if action == "open":
        conn = db()
        role_row = conn.execute(
            "SELECT role FROM gate_roles WHERE name=?",
            (name,)
        ).fetchone()
        conn.close()
        gate_role = role_row["role"] if role_row else "unknown"

        if gate_role == "exit":
            with state_lock:
                active = exit_active.get(name)
            active_car = get_car(active["plate"]) if active else None

            if not active_car or active_car.get("payment_status") != "PAID":
                audit(
                    session["user"], "EXIT_GATE_OPEN_BLOCKED", name,
                    "Payment interlock: no authorised PAID vehicle is active.",
                    "DENIED", simulator_now()
                )
                upsert_alert(
                    f"MANUAL_EXIT_INTERLOCK:{name}",
                    "HIGH", "EXIT GATE OPEN BLOCKED",
                    "Manual open denied: PARKMIND requires a valid paid vehicle before exit-gate release.",
                    simulator_now(), component=name
                )
                return redirect_back()

        blocker = gate_has_unpaid_blocker(name)
        if blocker:
            audit(
                session["user"], "EXIT_GATE_OPEN_BLOCKED", name,
                f"Unpaid blocker={blocker}", "DENIED", simulator_now()
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


@app.route("/control/payment/<plate>/retry", methods=["POST"])
def retry_payment(plate):
    if not require_roles("Operator", "Admin"):
        return ("Forbidden", 403)
    car = get_car(plate)
    if not car:
        return ("Unknown car", 404)
    if car.get("status") not in ("AT_EXIT", "PAYMENT_PENDING", "PAYMENT_HOLD"):
        return redirect_back()
    try:
        upsert_car(plate, payment_status="REQUESTED", status="PAYMENT_PENDING")
        charge_car(
            plate,
            float(car.get("parking_cost") or 0),
            float(car.get("charging_cost") or 0)
        )
        audit(session["user"], "PAYMENT_RETRY", plate, "", "OK", simulator_now())
    except Exception as e:
        upsert_car(plate, payment_status="CHARGE_ERROR", status="PAYMENT_HOLD")
        audit(session["user"], "PAYMENT_RETRY", plate, str(e), "ERROR", simulator_now())
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
    row = component_row(name, kind)
    if not row:
        return ("Unknown component", 404)

    reason = "Authorized manual repair"
    result = "COMMAND_SENT"
    try:
        component_action(kind, name, "repair", session["user"], simulator_now())
    except Exception as e:
        result = f"ERROR: {e}"

    conn = db()
    conn.execute(
        """INSERT INTO maintenance_actions(
           created_at,simulator_time,actor,component,kind,reason,action,result
           ) VALUES(?,?,?,?,?,?,?,?)""",
        (
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            simulator_now() or None,
            session["user"], name, kind, reason, "REPAIR", result
        )
    )
    conn.commit()
    conn.close()
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
        print("[PARKMIND] Level 2 topology loaded from simulator APIs.")
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
    print("====================================================\n")

    app.run(host="0.0.0.0", port=WEB_PORT, threaded=True)
