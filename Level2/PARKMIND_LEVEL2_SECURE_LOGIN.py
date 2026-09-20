# =====================================================================
#  PARKMIND v2.9  —  LEVEL 2 API-DRIVEN "Entry-Spawn Safe" Edition
#  Team: Pretty Little Hackers  |  Level: 2 — API-Driven
#  Team members: Sharmila, Jushita, Pravir, Ram
# =====================================================================
#  INNOVATIONS (pitch-worthy "wow" factors):
#
#  [1]  Webhook Signature Verification (MD5) — validates simulator-protocol
#       event integrity when a signature is provided; rejects mismatches.
#  [2]  Sequence Gap Detection + Auto Re-Sync — detects dropped
#       webhooks via SequenceId deltas; triggers defensive sync.
#  [3]  Multi-Factor Payment Verification — plate-tracked + amount-
#       matched + timing-consistent + no-double-charge.
#  [4]  Spot Reservation TTL with Auto-Release — locks spot for
#       arriving car; auto-releases after 25s if car never arrives.
#  [5]  Smart Spot Selection (5-Factor Scoring): compatibility +
#       zone balance + health + rotation + proximity-to-exit.
#  [6]  Self-Healing API Client — auto token refresh + exponential
#       backoff retry; never crashes on transient failures.
#  [7]  Decision Audit Trail w/ Human Reasoning — every AI choice
#       logged with WHY (judges can SEE the AI thinking).
#  [8]  Per-Spot Lifecycle Analytics — revenue/duration/utilization
#       per spot; identifies "money maker" vs "idle" spots.
#  [9]  Ghost-Car Payment Rejection — payments for untracked plates
#       instantly rejected + flagged as fraud attempt.
#  [10] Anomaly Detection Engine — flags 10x-long parkings, stuck
#       gates, slow transit, abnormal patterns.
#  [11] Auto-Stuck-State Recovery Watchdog — background thread
#       detects cars stuck in transient states and retries.
#  [12] Energy-Aware Future-Ready Hooks — logs CO2 + light state
#       even in Level 1; shows Level 2/3 readiness.
#  [13] Operator Annotations + Manual Override — operators can
#       attach notes; system respects manual annotations.
#  [14] CSV Export of Full Audit Log — compliance-grade export.
#  [15] Component Health Score (0–100) — predictive maintenance
#       preview; judges see we're thinking ahead.
# =====================================================================

from flask import Flask, request, jsonify, render_template_string, redirect, url_for, session, Response, flash
import requests
import sqlite3
import threading
import json
import math
import re
import hashlib
import csv
import io
import time
import random
import os
import hmac
import base64
import secrets
from datetime import datetime, timedelta
from urllib.parse import quote
from collections import defaultdict, Counter

# ---------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------
# Simulator connection. Component names/states are NEVER hardcoded.
# Credentials may be supplied by environment; defaults match the simulator docs.
SIM_BASE = os.getenv("PARKMIND_SIM_BASE", "http://127.0.0.1:9898/api/v1")
SIM_USER = os.getenv("PARKMIND_SIM_USER", "admin")
SIM_PASSWORD = os.getenv("PARKMIND_SIM_PASSWORD", "admin")

# These are API-DERIVED compatibility aliases only. They are populated from
# /list-barriers at level load; no gate name appears in source code.
ENTRY_GATE = None
EXIT_GATE = None
GATE_AUTO_DISCOVERY = True

# CRITICAL SPAWN-ROUTE SAFETY INVARIANT.
# The simulator attempts to create new arrivals BEFORE PARKMIND receives an
# EntrySpot webhook. If the entrance perimeter barrier is closed at that moment,
# the simulator cannot even create the vehicle and reports:
#     [ERROR] Won't spawn car No path from A to P2
# Therefore the API-discovered ENTRY_GATE is NEVER automatically or manually
# closed while PARKMIND is running. This is intentionally stronger than the old
# best-effort keep-open policy: every generic idle/exit cleanup path is prevented
# from closing the spawn-critical gate. The EXIT barrier remains independently
# controllable for the payment interlock.
KEEP_ENTRY_GATE_OPEN = True
KEEP_OPEN_GATES_ENV = [g.strip() for g in os.getenv("PARKMIND_KEEP_OPEN_GATES", "").split(",") if g.strip()]

# Level-2 policy settings. These are controller policies, not simulator data.
DAY_START_HOUR = int(os.getenv("PARKMIND_DAY_START_HOUR", "6"))
DAY_END_HOUR = int(os.getenv("PARKMIND_DAY_END_HOUR", "18"))
PREVENTIVE_MAINTENANCE_HEALTH = int(os.getenv("PARKMIND_PREVENTIVE_HEALTH", "15"))

WEB_PORT = 8000

# One shared SQLite database for the parking controller + maintenance dashboard.
# Using an absolute path avoids accidentally creating two DB files when the app
# is launched from a different working directory.
DEFAULT_DB_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "parkmind_level2_v6.db"
)
os.environ.setdefault("PARKMIND_LEVEL2_DB", DEFAULT_DB_PATH)

# Smart-selection tuning
RESERVATION_TTL_SEC = 25           # auto-release a reserved spot after 25s
STUCK_STATE_TIMEOUT_SEC = 60      # watchdog retries ops stuck > 60s
ANOMALY_PARKING_MULTIPLIER = 10   # 10x planned duration = anomaly
HEALTH_SCORE_THRESHOLD_USES = 50  # rough estimate for component "wear"
PAYMENT_TIMEOUT_SEC = 20            # unresolved exit payment -> alert
# /charge can return HTTP 201 before the simulator has internally settled the
# vehicle into its chargeable "waiting at exit" state.  The first request is
# deliberately delayed; retries are ONLY armed by the simulator's explicit
# "Car should be charged at the exit" rejection penalty.  This avoids both
# the early-charge race and accidental double charging.
CHARGE_RETRY_DELAYS = (1.0, 1.5, 2.0, 3.0)
EXIT_PAYMENT_RETRY_SEC = 2.0        # legacy tuning; targeted retry uses CHARGE_RETRY_DELAYS
EXIT_HOLD_RECOVERY_SEC = 180        # try to clear exit lane after prolonged unresolved payment
EXIT_STUCK_ALERT_SEC = 15           # create/update CRITICAL alert after 15s at EXIT without departure
ENTRY_GATE_IDLE_CLOSE_SEC = 1.0     # keep entry gate open briefly for back-to-back arrivals
EXIT_STAGE_A_CLEARANCE_SEC = float(os.getenv("PARKMIND_EXIT_STAGE_A_CLEARANCE_SEC", "0.8"))
LEAVE_PARKING_CLEARANCE_SEC = float(os.getenv("PARKMIND_LEAVE_CLEARANCE_SEC", "1.0"))
GATE_RELEASE_FALLBACK_SEC = 0.8     # route car if gate webhook is late
ENTRY_TRANSIT_TIMEOUT_SEC = 8        # car got a route command but never reached its spot
ENTRY_TRANSIT_MAX_RETRIES = 2        # resend assigned destination before aborting that one car
ZONE_GATE_CLEARANCE_SEC = float(os.getenv("PARKMIND_ZONE_CLEARANCE_SEC", "1.5"))  # after EntrySpot CarOut; do not wait for parking

# Judge/testing state. Normal parking is NEVER forced full by a source-code flag.
# A live full-lot test can be armed for ONE incoming car from the Admin dashboard.
demo_state = {
    "enabled": False,
    "last_scenario": None,
    "events": [],
    "live_full_next_arrival": False
}

# ---------------------------------------------------------------------
# APP
# ---------------------------------------------------------------------
app = Flask(__name__)

# Level-2 login/session security.  A deployment should set PARKMIND_SECRET_KEY
# so sessions survive process restarts; local hackathon runs get a strong
# per-process fallback instead of a hardcoded source-code secret.
app.secret_key = os.getenv("PARKMIND_SECRET_KEY") or secrets.token_hex(32)
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=(os.getenv("PARKMIND_HTTPS", "0").strip() == "1"),
    PERMANENT_SESSION_LIFETIME=timedelta(minutes=30),
    SESSION_REFRESH_EACH_REQUEST=True,
)

token = None
token_lock = threading.Lock()

# Live simulator cache. Initial values come from discovery GET endpoints once per
# level load/recovery; webhooks and successful POST commands keep this cache live.
spots = {}             # Park-purpose spots only
parking_nodes = {}     # every item returned by /list-parking-spots
entry_spots = {}       # purpose == EntrySpot
exit_spots = {}        # purpose == ExitSpot
leave_spots = {}       # purpose == LeaveParking
gates = {}             # /list-barriers
zones = {}             # /list-zones
lights = {}            # /list-lights
fans = {}              # /list-exhaust-fans
alarms = {}            # /list-alarms, keyed by component name

# API-derived topology indexes. The supplied API does not expose an explicit
# "entry barrier" / "exit barrier" role, so perimeter gates are identified only
# from zoneParent == "" and zone gates from their returned zoneParent.
perimeter_gates = []
zone_gates = defaultdict(list)

# Local operator intent is not simulator state. AUTO means PARKMIND policies may
# control that component; ON/OFF means the operator explicitly overrode it.
manual_overrides = {"gate": {}, "fan": {}, "light": {}, "light_group": {}}
maintenance_requested = set()
last_simulator_time = None
last_simulator_time_seen_at = None  # local monotonic time when webhook clock was received
last_sync_at = None

# One active crossing slot per zone prevents cars from piling at an internal
# zone barrier. The slot is released shortly after a successfully dispatched car
# clears the gate path; we NEVER wait for the car to finish parking.
# Zone names are discovered from the API; nothing is hardcoded.
zone_entry_active = {}   # zone -> plate; at most one inbound car may occupy a zone gate slot
exit_route_zone = {}      # plate -> zone; exactly one outbound car per zone barrier path

reserved_spots = {}    # spot_name -> {"plate", "reserved_at"}
entry_queue = []

# IMPORTANT PIPELINE MODEL:
# entry_active = ONLY the vehicle currently crossing the entry gate.
# in_transit   = vehicles that already cleared an API-discovered EntrySpot and are driving to reserved bays.
entry_active = None
in_transit = {}           # plate -> {"spot", "sent_at", "route_retries"}

exit_active  = None
# EXIT SAFETY MODEL:
# Exactly ONE vehicle may be released from a parking bay toward the physical
# ExitSpot area at a time. Every other due vehicle remains parked in its bay.
# This prevents the simulator's generic goto/exit routing from stacking many
# cars at the same exit.
exit_lane_plate = None
api_exit_unknown_count = 0  # API-reported ExitSpot occupants when plate IDs are unavailable
# PRACTICAL TWO-STAGE EXIT PIPELINE:
#   Stage A: exactly one car may own/occupy the ExitSpot payment bay.
#   Stage B: exactly one paid car may occupy the escape corridor toward LeaveParking.
# A new car may enter Stage A as soon as the previous car leaves ExitSpot, even
# while that previous car is still finishing Stage B. This prevents pile-up
# without forcing the whole facility to wait for final LeaveParking clearance.
escape_corridor_plate = None
api_escape_unknown_count = 0
to_exit_queue = []        # due cars waiting safely in their parking bays
physical_exit_queue = []  # Stage-A cars; should normally contain 0 or 1 car
exit_spot_by_plate = {}    # plate -> actual API-reported ExitSpot name
# Number of /charge requests actually sent for the active trip of each plate.
# A second request is never scheduled merely because HTTP 201 was returned; the
# simulator must explicitly reject the previous attempt before this counter is
# allowed to advance to the next backoff slot.
charge_attempt_counts = {}
charge_attempt_scheduled = set()
leave_finalize_inflight = set()

state_lock = threading.RLock()

# Stats counters
stats = {
    "total_arrivals": 0,
    "total_parked": 0,
    "total_departed": 0,
    "total_revenue": 0.0,
    "total_penalties": 0,
    "fraud_attempts_blocked": 0,
    "webhooks_verified": 0,
    "webhooks_unsigned_accepted": 0,  # legacy counter retained for compatibility
    "webhooks_unsigned_rejected": 0,
    "webhooks_rejected_sig": 0,
    "sequence_gaps_detected": 0,
    "auto_recoveries": 0,
    "anomalies_flagged": 0,
}

# Track last seen SequenceId for gap detection
last_sequence_id = None
sequence_lock = threading.Lock()


# =====================================================================
# DATABASE
# =====================================================================
def db():
    conn = sqlite3.connect(os.getenv("PARKMIND_LEVEL2_DB", "parkmind_level2_v6.db"), timeout=10)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = db()
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id TEXT UNIQUE,
        sequence_id INTEGER,
        event_class TEXT,
        server_time TEXT,
        signature_verified INTEGER DEFAULT 0,
        payload TEXT
    );

    CREATE TABLE IF NOT EXISTS cars (
        plate TEXT PRIMARY KEY,
        car_type TEXT,
        planned_minutes INTEGER DEFAULT 0,
        assigned_spot TEXT,
        actual_spot TEXT,
        route_mismatch_count INTEGER DEFAULT 0,
        entry_time TEXT,
        parked_time TEXT,
        exit_arrival_time TEXT,
        departure_time TEXT,
        expected_amount REAL DEFAULT 0,
        actual_paid REAL DEFAULT 0,
        billable_minutes INTEGER DEFAULT 0,
        total_stay_minutes INTEGER DEFAULT 0,
        total_stay_seconds INTEGER DEFAULT 0,
        parking_cost REAL DEFAULT 0,
        charging_cost REAL DEFAULT 0,
        total_charge REAL DEFAULT 0,
        payment_status TEXT DEFAULT 'NONE',
        payment_factors TEXT,
        status TEXT DEFAULT 'NEW',
        decision TEXT,
        operator_note TEXT,
        anomaly_flag INTEGER DEFAULT 0,
        journey TEXT
    );

    CREATE TABLE IF NOT EXISTS decisions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT,
        plate TEXT,
        action TEXT,
        detail TEXT,
        reasoning TEXT
    );

    CREATE TABLE IF NOT EXISTS spot_stats (
        spot TEXT PRIMARY KEY,
        cars_hosted INTEGER DEFAULT 0,
        total_revenue REAL DEFAULT 0,
        total_minutes INTEGER DEFAULT 0,
        first_used TEXT,
        last_used TEXT
    );

    CREATE TABLE IF NOT EXISTS anomalies (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        detected_at TEXT,
        plate TEXT,
        kind TEXT,
        detail TEXT
    );

    CREATE TABLE IF NOT EXISTS fraud_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        detected_at TEXT,
        plate TEXT,
        reason TEXT,
        attempted_amount REAL
    );

    CREATE TABLE IF NOT EXISTS penalties (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        detected_at TEXT,
        event_time TEXT,
        plate TEXT,
        reason TEXT,
        fine_amount REAL,
        payload TEXT
    );

    CREATE TABLE IF NOT EXISTS alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        alert_key TEXT UNIQUE,
        created_at TEXT,
        severity TEXT,
        alert_type TEXT,
        plate TEXT,
        reason TEXT,
        entry_time TEXT,
        event_time TEXT,
        exit_arrival_time TEXT,
        stay_minutes INTEGER DEFAULT 0,
        stay_seconds INTEGER DEFAULT 0,
        stuck_seconds INTEGER DEFAULT 0,
        assigned_spot TEXT,
        expected_amount REAL DEFAULT 0,
        actual_paid REAL DEFAULT 0,
        fine_amount REAL DEFAULT 0,
        payment_status TEXT,
        car_status TEXT
    );

    -- Level-2 security/audit additions are deliberately separate tables so
    -- they cannot disturb the working vehicle/payment schema.
    CREATE TABLE IF NOT EXISTS login_attempts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        attempted_at TEXT,
        username TEXT,
        success INTEGER DEFAULT 0,
        role TEXT,
        ip TEXT
    );

    CREATE TABLE IF NOT EXISTS login_security (
        username TEXT NOT NULL,
        ip TEXT NOT NULL,
        failed_attempts INTEGER DEFAULT 0,
        locked_until TEXT,
        last_attempt TEXT,
        PRIMARY KEY(username, ip)
    );

    CREATE TABLE IF NOT EXISTS unsigned_webhooks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        received_at TEXT,
        event_id TEXT,
        sequence_id INTEGER,
        event_class TEXT,
        payload TEXT
    );

    CREATE TABLE IF NOT EXISTS audit_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT,
        actor TEXT,
        role TEXT,
        action TEXT,
        target TEXT,
        detail TEXT,
        result TEXT
    );

    CREATE TABLE IF NOT EXISTS manual_recoveries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT,
        actor TEXT,
        plate TEXT,
        car_type TEXT,
        spot TEXT,
        estimated_minutes INTEGER,
        recovery_state TEXT,
        detail TEXT
    );
    """)
    # Safe migration for an existing parkmind_v2.db created by an older build.
    existing_cols = {row[1] for row in conn.execute("PRAGMA table_info(cars)").fetchall()}
    required_cols = {
        "billable_minutes": "INTEGER DEFAULT 0",
        "total_stay_minutes": "INTEGER DEFAULT 0",
        "total_stay_seconds": "INTEGER DEFAULT 0",
        "parking_cost": "REAL DEFAULT 0",
        "charging_cost": "REAL DEFAULT 0",
        "total_charge": "REAL DEFAULT 0",
        "actual_spot": "TEXT",
        "route_mismatch_count": "INTEGER DEFAULT 0",
    }
    for col, ddl in required_cols.items():
        if col not in existing_cols:
            conn.execute(f"ALTER TABLE cars ADD COLUMN {col} {ddl}")

    penalty_cols = {row[1] for row in conn.execute("PRAGMA table_info(penalties)").fetchall()}
    if "event_time" not in penalty_cols:
        conn.execute("ALTER TABLE penalties ADD COLUMN event_time TEXT")
    if "plate" not in penalty_cols:
        conn.execute("ALTER TABLE penalties ADD COLUMN plate TEXT")

    alert_cols = {row[1] for row in conn.execute("PRAGMA table_info(alerts)").fetchall()}
    if "stay_seconds" not in alert_cols:
        conn.execute("ALTER TABLE alerts ADD COLUMN stay_seconds INTEGER DEFAULT 0")
    if "exit_arrival_time" not in alert_cols:
        conn.execute("ALTER TABLE alerts ADD COLUMN exit_arrival_time TEXT")
    if "stuck_seconds" not in alert_cols:
        conn.execute("ALTER TABLE alerts ADD COLUMN stuck_seconds INTEGER DEFAULT 0")

    conn.commit()
    conn.close()


def record_login_attempt(username, success, role=""):
    """Persist every successful/failed login without ever blocking authentication."""
    ip = (request.headers.get("X-Forwarded-For") or request.remote_addr or "").split(",")[0].strip()
    conn = None
    try:
        conn = db()
        conn.execute(
            "INSERT INTO login_attempts(attempted_at,username,success,role,ip) VALUES(?,?,?,?,?)",
            (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), str(username or ""), 1 if success else 0, str(role or ""), ip)
        )
        conn.commit()
    except Exception as e:
        print("[LOGIN AUDIT]", e)
    finally:
        if conn is not None:
            conn.close()


def recent_login_attempts(limit=3):
    conn = db()
    try:
        rows = [dict(r) for r in conn.execute(
            "SELECT attempted_at,username,success,role,ip FROM login_attempts ORDER BY id DESC LIMIT ?",
            (max(1, int(limit)),)
        ).fetchall()]
    except Exception:
        rows = []
    conn.close()
    return rows


# ---------------------------------------------------------------------
# LEVEL-2 LOGIN SECURITY
# ---------------------------------------------------------------------
LOGIN_MAX_ATTEMPTS = int(os.getenv("PARKMIND_LOGIN_MAX_ATTEMPTS", "5"))
LOGIN_LOCK_MINUTES = int(os.getenv("PARKMIND_LOGIN_LOCK_MINUTES", "5"))
PASSWORD_HASH_ITERATIONS = int(os.getenv("PARKMIND_PASSWORD_HASH_ITERATIONS", "200000"))


def _hash_password(password, salt=None, iterations=None):
    """Return a portable salted PBKDF2-SHA256 password hash using stdlib only."""
    iterations = int(iterations or PASSWORD_HASH_ITERATIONS)
    salt = salt or secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256", str(password).encode("utf-8"), salt, iterations
    )
    return "pbkdf2_sha256${}${}${}".format(
        iterations,
        base64.b64encode(salt).decode("ascii"),
        base64.b64encode(digest).decode("ascii"),
    )


def _verify_password(password, encoded):
    """Constant-time verification for PARKMIND's PBKDF2 password format."""
    try:
        algorithm, iterations_text, salt_b64, digest_b64 = str(encoded).split("$", 3)
        if algorithm != "pbkdf2_sha256":
            return False
        iterations = int(iterations_text)
        salt = base64.b64decode(salt_b64.encode("ascii"), validate=True)
        expected = base64.b64decode(digest_b64.encode("ascii"), validate=True)
        actual = hashlib.pbkdf2_hmac(
            "sha256", str(password).encode("utf-8"), salt, iterations
        )
        return hmac.compare_digest(actual, expected)
    except Exception:
        return False


def _client_ip():
    forwarded = request.headers.get("X-Forwarded-For", "")
    if forwarded:
        return forwarded.split(",")[0].strip()[:64]
    return str(request.remote_addr or "UNKNOWN")[:64]


def _login_security_state(username, ip):
    conn = db()
    try:
        row = conn.execute(
            "SELECT failed_attempts,locked_until,last_attempt FROM login_security "
            "WHERE username=? AND ip=?",
            (str(username), str(ip)),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def _reset_login_security(username, ip):
    conn = db()
    try:
        conn.execute(
            "DELETE FROM login_security WHERE username=? AND ip=?",
            (str(username), str(ip)),
        )
        conn.commit()
    finally:
        conn.close()


def _login_lock_status(username, ip):
    """Return (locked, seconds_remaining). Expired lockouts auto-clear."""
    state = _login_security_state(username, ip)
    if not state or not state.get("locked_until"):
        return False, 0
    try:
        unlock_at = datetime.strptime(state["locked_until"], "%Y-%m-%d %H:%M:%S")
        remaining = int((unlock_at - datetime.now()).total_seconds())
        if remaining > 0:
            return True, remaining
    except Exception:
        pass
    _reset_login_security(username, ip)
    return False, 0


def _register_failed_auth(username, ip):
    """Increment one username+IP failure bucket and arm a temporary lockout."""
    now = datetime.now()
    state = _login_security_state(username, ip) or {}
    failures = int(state.get("failed_attempts") or 0) + 1
    locked_until = None
    if failures >= max(1, LOGIN_MAX_ATTEMPTS):
        locked_until = (now + timedelta(minutes=max(1, LOGIN_LOCK_MINUTES))).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

    conn = db()
    try:
        conn.execute(
            """INSERT INTO login_security(username,ip,failed_attempts,locked_until,last_attempt)
               VALUES(?,?,?,?,?)
               ON CONFLICT(username,ip) DO UPDATE SET
                   failed_attempts=excluded.failed_attempts,
                   locked_until=excluded.locked_until,
                   last_attempt=excluded.last_attempt""",
            (
                str(username), str(ip), failures, locked_until,
                now.strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )
        conn.commit()
    finally:
        conn.close()
    return failures, locked_until


def _authenticate_user(username, password):
    """Verify credentials without revealing whether the username exists."""
    user = USERS.get(username)
    stored_hash = user.get("password_hash") if user else DUMMY_PASSWORD_HASH
    valid = _verify_password(password, stored_hash)
    return user if (user is not None and valid) else None


def log_unsigned_webhook(data):
    """Store unsigned Level-2 calls for security review; never passes them to handle_event."""
    conn = db()
    conn.execute(
        """INSERT INTO unsigned_webhooks(received_at,event_id,sequence_id,event_class,payload)
           VALUES(?,?,?,?,?)""",
        (
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            str(data.get("EventId") or ""), data.get("SequenceId"),
            str(data.get("EventClass") or ""), json.dumps(data)
        )
    )
    conn.commit()
    conn.close()


def log_audit(action, target="", detail="", result="SUCCESS", actor=None, role=None):
    """Level-2 actor-aware audit record. Audit failure must never stop parking control."""
    if actor is None:
        try:
            actor = session.get("user") or "SYSTEM"
            role = role or session.get("role") or "SYSTEM"
        except RuntimeError:
            actor, role = "SYSTEM", (role or "SYSTEM")
    conn = None
    try:
        conn = db()
        conn.execute(
            "INSERT INTO audit_log(created_at,actor,role,action,target,detail,result) VALUES(?,?,?,?,?,?,?)",
            (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), str(actor), str(role or ""),
             str(action), str(target or ""), str(detail or ""), str(result or ""))
        )
        conn.commit()
    except Exception as e:
        print("[AUDIT]", e)
    finally:
        if conn is not None:
            conn.close()


def seconds_between(start_text, end_text):
    """Exact non-negative seconds between two simulator timestamps."""
    if not start_text or not end_text:
        return 0
    try:
        start_dt = datetime.strptime(str(start_text).replace("T", " ")[:19], "%Y-%m-%d %H:%M:%S")
        end_dt = datetime.strptime(str(end_text).replace("T", " ")[:19], "%Y-%m-%d %H:%M:%S")
        return max(0, int((end_dt - start_dt).total_seconds()))
    except Exception:
        return 0


def format_duration(seconds):
    """Human-readable exact duration: 42s, 3m 12s, 1h 05m."""
    try:
        seconds = max(0, int(seconds or 0))
    except Exception:
        seconds = 0

    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)

    if hours:
        return f"{hours}h {minutes:02d}m {secs:02d}s"
    if minutes:
        return f"{minutes}m {secs:02d}s"
    return f"{secs}s"


def log_decision(plate, action, detail, reasoning=""):
    conn = db()
    conn.execute(
        "INSERT INTO decisions(created_at, plate, action, detail, reasoning) VALUES(?,?,?,?,?)",
        (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), plate, action, detail, reasoning)
    )
    conn.commit()
    conn.close()
    print(f"[DECISION] {plate or '-'} | {action} | {detail} | {reasoning}")


def log_anomaly(plate, kind, detail):
    conn = db()
    conn.execute(
        "INSERT INTO anomalies(detected_at, plate, kind, detail) VALUES(?,?,?,?)",
        (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), plate, kind, detail)
    )
    conn.commit()
    conn.close()
    with state_lock:
        stats["anomalies_flagged"] += 1
    print(f"[ANOMALY] {plate or '-'} | {kind} | {detail}")


def log_fraud(plate, reason, attempted_amount):
    conn = db()
    conn.execute(
        "INSERT INTO fraud_log(detected_at, plate, reason, attempted_amount) VALUES(?,?,?,?)",
        (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), plate, reason, attempted_amount)
    )
    conn.commit()
    conn.close()
    with state_lock:
        stats["fraud_attempts_blocked"] += 1
    print(f"[FRAUD BLOCKED] {plate or '-'} | {reason} | amount={attempted_amount}")


def log_penalty(reason, fine_amount, payload=None, plate=""):
    payload = payload or {}
    conn = db()
    conn.execute(
        """INSERT INTO penalties(detected_at, event_time, plate, reason, fine_amount, payload)
           VALUES(?,?,?,?,?,?)""",
        (
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            str(payload.get("ServerDateTime") or ""),
            str(plate or payload.get("CarPlateNumber") or payload.get("PlateNumber") or ""),
            str(reason or ""),
            float(fine_amount or 0),
            json.dumps(payload)
        )
    )
    conn.commit()
    conn.close()
    with state_lock:
        stats["total_penalties"] += 1
    print(f"[PENALTY] {reason} | fine={fine_amount}")


def save_event(data, verified=False):
    event_id = str(data.get("EventId") or "")
    conn = db()
    try:
        conn.execute(
            """INSERT INTO events(event_id, sequence_id, event_class, server_time, signature_verified, payload)
               VALUES(?,?,?,?,?,?)""",
            (
                event_id,
                data.get("SequenceId"),
                data.get("EventClass"),
                data.get("ServerDateTime"),
                1 if verified else 0,
                json.dumps(data)
            )
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()


def upsert_car(plate, **fields):
    conn = db()
    row = conn.execute("SELECT plate FROM cars WHERE plate=?", (plate,)).fetchone()
    if not row:
        conn.execute("INSERT INTO cars(plate) VALUES(?)", (plate,))
    if fields:
        cols = ", ".join(f"{k}=?" for k in fields.keys())
        vals = list(fields.values()) + [plate]
        conn.execute(f"UPDATE cars SET {cols} WHERE plate=?", vals)
    conn.commit()
    conn.close()


def get_car(plate):
    conn = db()
    row = conn.execute("SELECT * FROM cars WHERE plate=?", (plate,)).fetchone()
    conn.close()
    return dict(row) if row else None


def infer_plate_for_alert(data=None, reason="", event_time=None):
    """
    Resolve the vehicle plate for simulator penalty/alert events.

    Some simulator penalty webhooks do not include CarPlateNumber.
    Resolution order:
      1) direct plate fields in the webhook
      2) plate-like text embedded in Reason/payload
      3) currently active exit/entry vehicle
      4) nearest relevant car in SQLite around the event time

    Returns "" when there is not enough evidence rather than inventing a plate.
    """
    data = data or {}
    reason = str(reason or "")

    # 1) Direct simulator fields.
    for key in (
        "CarPlateNumber", "PlateNumber", "CarPlate", "Plate",
        "CarName", "VehiclePlate", "Vehicle"
    ):
        value = data.get(key)
        if value:
            return str(value).strip()

    # 2) Search the reason + serialized payload for a Malaysian-style/simple plate token.
    haystack = reason + " " + json.dumps(data, ensure_ascii=False)
    # Covers examples like ABC 123, WKV395, XLC 191.
    plate_matches = re.findall(r"\b[A-Z]{1,4}\s?\d{1,4}\b", haystack.upper())
    if plate_matches:
        candidate = plate_matches[0].strip()
        # Normalize "ABC123" -> "ABC 123" where possible to match DB.
        m = re.match(r"^([A-Z]{1,4})\s?(\d{1,4})$", candidate)
        if m:
            compact = f"{m.group(1)} {m.group(2)}"
            # Prefer exact DB representation if present.
            conn = db()
            row = conn.execute(
                "SELECT plate FROM cars WHERE REPLACE(UPPER(plate),' ','')=? LIMIT 1",
                (f"{m.group(1)}{m.group(2)}",)
            ).fetchone()
            conn.close()
            if row:
                return row["plate"]
            return compact

    lower_reason = reason.lower()

    # 3) Current live controller state is strong evidence for entry/exit penalties.
    with state_lock:
        current_exit = exit_active.get("plate") if exit_active else None
        current_exit_lane = physical_exit_queue[0] if physical_exit_queue else None
        current_entry = entry_active.get("plate") if entry_active else None

    exit_words = (
        "pay", "payment", "charge", "exit", "escape", "unpaid",
        "electricity", "double charge", "incorrect amount"
    )
    entry_words = ("entry", "entrance", "gate", "neglected entry")

    if any(word in lower_reason for word in exit_words):
        if current_exit:
            return current_exit
        if current_exit_lane:
            return current_exit_lane

    if any(word in lower_reason for word in entry_words) and current_entry:
        return current_entry

    # 4) Database temporal/context correlation.
    # Penalties in L1 are generally caused by a car participating in the current lifecycle.
    conn = db()

    target_dt = None
    if event_time:
        try:
            target_dt = datetime.strptime(str(event_time)[:19], "%Y-%m-%d %H:%M:%S")
        except Exception:
            target_dt = None

    if any(word in lower_reason for word in exit_words):
        rows = conn.execute(
            """SELECT * FROM cars
               WHERE status IN (
                   'AT_EXIT','PAYMENT_PENDING','PAYMENT_HOLD','PAID',
                   'ESCAPED_UNPAID','LEFT'
               )
               ORDER BY COALESCE(exit_arrival_time, departure_time, entry_time, '') DESC
               LIMIT 10"""
        ).fetchall()
    else:
        rows = conn.execute(
            """SELECT * FROM cars
               WHERE status NOT IN ('LEFT')
               ORDER BY COALESCE(entry_time,'') DESC
               LIMIT 10"""
        ).fetchall()

    conn.close()

    if not rows:
        return ""

    # If timestamp is available, choose the car with the nearest relevant timestamp
    # but only inside a conservative 90-second window.
    if target_dt:
        best_plate = ""
        best_delta = None
        for row in rows:
            d = dict(row)
            candidate_times = [
                d.get("departure_time"),
                d.get("exit_arrival_time"),
                d.get("parked_time"),
                d.get("entry_time"),
            ]
            for ts in candidate_times:
                if not ts:
                    continue
                try:
                    dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
                    delta = abs((target_dt - dt).total_seconds())
                    if best_delta is None or delta < best_delta:
                        best_delta = delta
                        best_plate = d.get("plate") or ""
                except Exception:
                    pass

        if best_plate and best_delta is not None and best_delta <= 90:
            return best_plate

    # Strong fallback only when exactly one relevant active car exists.
    active_plates = []
    for row in rows:
        d = dict(row)
        if d.get("status") in (
            "AT_EXIT","PAYMENT_PENDING","PAYMENT_HOLD","PAID",
            "WAITING","ASSIGNED","REROUTING","TO_EXIT","PARKED"
        ):
            p = d.get("plate")
            if p and p not in active_plates:
                active_plates.append(p)

    return active_plates[0] if len(active_plates) == 1 else ""


def refresh_alert_from_car(alert_id, plate):
    """Backfill/enrich an existing alert after its plate has been resolved."""
    if not plate:
        return
    car = get_car(plate) or {}

    event_time = None
    conn = db()
    alert = conn.execute(
        "SELECT * FROM alerts WHERE id=?",
        (alert_id,)
    ).fetchone()
    if alert:
        event_time = alert["event_time"]

    entry_time = car.get("entry_time")
    stay_seconds = int(car.get("total_stay_seconds") or 0)

    if not stay_seconds and entry_time and event_time:
        stay_seconds = seconds_between(entry_time, event_time)

    stay_minutes = (
        max(1, math.ceil(stay_seconds / 60))
        if stay_seconds > 0
        else int(car.get("total_stay_minutes") or car.get("billable_minutes") or 0)
    )

    conn.execute(
        """UPDATE alerts
           SET plate=?,
               entry_time=COALESCE(?, entry_time),
               stay_minutes=?,
               stay_seconds=?,
               assigned_spot=COALESCE(?, assigned_spot),
               expected_amount=?,
               actual_paid=?,
               payment_status=COALESCE(?, payment_status),
               car_status=COALESCE(?, car_status)
           WHERE id=?""",
        (
            plate,
            entry_time,
            stay_minutes,
            stay_seconds,
            car.get("assigned_spot"),
            float(car.get("expected_amount") or car.get("total_charge") or 0),
            float(car.get("actual_paid") or 0),
            car.get("payment_status"),
            car.get("status"),
            alert_id,
        )
    )
    conn.commit()
    conn.close()


def upsert_exit_stuck_alert(plate):
    """Create/update one live CRITICAL alert per vehicle stuck at EXIT."""
    car = get_car(plate) or {}
    exit_time = car.get("exit_arrival_time")
    entry_time = car.get("entry_time")
    if not exit_time or car.get("departure_time"):
        return

    now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    stuck_seconds = seconds_between(exit_time, now_text)
    stay_seconds = seconds_between(entry_time, now_text) if entry_time else 0

    if stuck_seconds < EXIT_STUCK_ALERT_SEC:
        return

    alert_key = f"EXIT_STUCK:{plate}"
    severity = "CRITICAL"
    reason = (
        f"Vehicle has remained at EXIT for {format_duration(stuck_seconds)} "
        f"without a confirmed departure. Status={car.get('status')}; "
        f"payment={car.get('payment_status')}."
    )

    conn = db()
    conn.execute(
        """INSERT INTO alerts(
            alert_key, created_at, severity, alert_type, plate, reason,
            entry_time, event_time, exit_arrival_time,
            stay_minutes, stay_seconds, stuck_seconds, assigned_spot,
            expected_amount, actual_paid, fine_amount, payment_status, car_status
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(alert_key) DO UPDATE SET
            created_at=excluded.created_at,
            severity=excluded.severity,
            alert_type=excluded.alert_type,
            plate=excluded.plate,
            reason=excluded.reason,
            entry_time=excluded.entry_time,
            event_time=excluded.event_time,
            exit_arrival_time=excluded.exit_arrival_time,
            stay_minutes=excluded.stay_minutes,
            stay_seconds=excluded.stay_seconds,
            stuck_seconds=excluded.stuck_seconds,
            assigned_spot=excluded.assigned_spot,
            expected_amount=excluded.expected_amount,
            actual_paid=excluded.actual_paid,
            payment_status=excluded.payment_status,
            car_status=excluded.car_status""",
        (
            alert_key,
            now_text,
            severity,
            "CAR STUCK AT EXIT",
            plate,
            reason,
            entry_time,
            now_text,
            exit_time,
            max(1, math.ceil(stay_seconds / 60)) if stay_seconds else 0,
            stay_seconds,
            stuck_seconds,
            car.get("assigned_spot"),
            float(car.get("expected_amount") or car.get("total_charge") or 0),
            float(car.get("actual_paid") or 0),
            0.0,
            car.get("payment_status"),
            car.get("status"),
        )
    )
    conn.commit()
    conn.close()


def resolve_exit_stuck_alert(plate):
    """Remove the live stuck-at-exit alert once the vehicle actually departs."""
    conn = db()
    conn.execute("DELETE FROM alerts WHERE alert_key=?", (f"EXIT_STUCK:{plate}",))
    conn.commit()
    conn.close()


def exit_stuck_alert_watchdog():
    """Continuously detect any car that is physically stuck at EXIT."""
    while True:
        time.sleep(2)
        try:
            conn = db()
            rows = conn.execute(
                """SELECT plate
                   FROM cars
                   WHERE exit_arrival_time IS NOT NULL
                     AND departure_time IS NULL
                     AND status IN (
                        'AT_EXIT','PAYMENT_PENDING','PAYMENT_HOLD',
                        'PAID','CHARGE_ERROR'
                     )"""
            ).fetchall()
            conn.close()

            for row in rows:
                upsert_exit_stuck_alert(row["plate"])

        except Exception as e:
            print("[EXIT STUCK WATCHDOG]", e)


def reconcile_unresolved_alerts():
    """Repair old alerts that were created before a plate was available."""
    conn = db()
    rows = conn.execute(
        """SELECT id, alert_type, reason, event_time, plate
           FROM alerts
           WHERE plate IS NULL OR TRIM(plate)='' OR UPPER(plate)='UNKNOWN'
           ORDER BY id DESC
           LIMIT 30"""
    ).fetchall()
    conn.close()

    repaired = 0
    for row in rows:
        resolved = infer_plate_for_alert(
            data={},
            reason=row["reason"] or row["alert_type"] or "",
            event_time=row["event_time"]
        )
        if resolved:
            refresh_alert_from_car(row["id"], resolved)
            repaired += 1

    if repaired:
        print(f"[ALERT RECONCILE] Repaired {repaired} alert plate(s).")


def record_alert(
    alert_key,
    alert_type,
    severity,
    plate="",
    reason="",
    event_time=None,
    fine_amount=0.0
):
    """Create a compact operational alert with the important car context."""
    event_time = event_time or datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Penalty webhooks do not always contain a plate. Resolve from live/DB context.
    if not plate:
        plate = infer_plate_for_alert(
            data={},
            reason=reason,
            event_time=event_time
        )

    car = get_car(plate) if plate else None
    car = car or {}

    entry_time = car.get("entry_time")
    stay_seconds = int(car.get("total_stay_seconds") or 0)

    # For an open trip, duration is entry -> alert/event time.
    if not stay_seconds and entry_time:
        stay_seconds = seconds_between(entry_time, event_time)

    stay_minutes = (
        max(1, math.ceil(stay_seconds / 60))
        if stay_seconds > 0
        else int(car.get("total_stay_minutes") or car.get("billable_minutes") or 0)
    )

    conn = db()
    conn.execute(
        """INSERT OR IGNORE INTO alerts(
            alert_key, created_at, severity, alert_type, plate, reason,
            entry_time, event_time, stay_minutes, stay_seconds, assigned_spot,
            expected_amount, actual_paid, fine_amount, payment_status, car_status
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            str(alert_key),
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            severity,
            alert_type,
            plate or "",
            reason or "",
            entry_time,
            event_time,
            stay_minutes,
            stay_seconds,
            car.get("assigned_spot"),
            float(car.get("expected_amount") or car.get("total_charge") or 0),
            float(car.get("actual_paid") or 0),
            float(fine_amount or 0),
            car.get("payment_status"),
            car.get("status")
        )
    )
    conn.commit()
    conn.close()

    print(
        f"[ALERT] {severity} | {alert_type} | {plate or 'UNKNOWN'} | "
        f"{reason} | stay={stay_minutes}min | fine={fine_amount}"
    )


def append_journey(plate, step):
    car = get_car(plate)
    if not car:
        return
    journey = car.get("journey") or ""
    parts = [p for p in journey.split(" -> ") if p]
    parts.append(f"{datetime.now().strftime('%H:%M:%S')}:{step}")
    upsert_car(plate, journey=" -> ".join(parts[-10:]))


def update_spot_stats(spot, minutes, revenue):
    conn = db()
    row = conn.execute("SELECT spot FROM spot_stats WHERE spot=?", (spot,)).fetchone()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if not row:
        conn.execute(
            "INSERT INTO spot_stats(spot, cars_hosted, total_revenue, total_minutes, first_used, last_used) VALUES(?,?,?,?,?,?)",
            (spot, 1, revenue, minutes, now, now)
        )
    else:
        conn.execute(
            "UPDATE spot_stats SET cars_hosted=cars_hosted+1, total_revenue=total_revenue+?, total_minutes=total_minutes+?, last_used=? WHERE spot=?",
            (revenue, minutes, now, spot)
        )
    conn.commit()
    conn.close()


# =====================================================================
# [INNOVATION #1] — WEBHOOK SIGNATURE VERIFICATION (MD5)
# =====================================================================
def verify_webhook_signature(data: dict):
    """
    Returns:
      True  -> a non-empty signature was present and matched.
      False -> a non-empty signature was present but DID NOT match.
      None  -> simulator sent no usable signature (e.g. Signature=None).

    Level 2 requires signed webhooks. Missing signatures are returned as None
    so the endpoint can log and reject them without altering controller state.
    """
    raw_sig = data.get("Signature")

    if raw_sig is None or str(raw_sig).strip().lower() in ("", "none", "null"):
        return None

    received_sig = str(raw_sig).strip().lower()
    payload = {k: v for k, v in data.items() if k != "Signature"}

    sorted_keys = sorted(payload.keys())
    values = []
    for k in sorted_keys:
        v = payload[k]
        if v is None:
            v = ""
        elif isinstance(v, float):
            v = repr(v)
        else:
            v = str(v)
        values.append(v)

    joined = "|".join(values)
    computed = hashlib.md5(joined.encode("utf-8")).hexdigest()
    return computed == received_sig


# =====================================================================
# [INNOVATION #2] — SEQUENCE GAP DETECTION + AUTO RE-SYNC
# =====================================================================
def check_sequence_gap(seq_id):
    """
    Tracks SequenceId deltas. If we observe a gap > 1, we missed an event.
    Triggers a defensive state-sync to recover.
    """
    global last_sequence_id
    if seq_id is None:
        return
    with sequence_lock:
        if last_sequence_id is not None and seq_id > last_sequence_id + 1:
            gap_size = seq_id - last_sequence_id - 1
            with state_lock:
                stats["sequence_gaps_detected"] += gap_size
            log_decision(
                "", "SEQUENCE_GAP",
                f"Missed {gap_size} event(s) between seq {last_sequence_id} and {seq_id}",
                "Defensive re-sync scheduled"
            )
            # Schedule a recovery sync in a background thread.
            threading.Timer(2.0, lambda: safe_sync_state("gap_recovery")).start()
        last_sequence_id = max(last_sequence_id or 0, seq_id)


# =====================================================================
# [INNOVATION #6] — SELF-HEALING API CLIENT
# =====================================================================
def sim_login():
    global token
    with token_lock:
        for attempt in range(3):
            try:
                r = requests.post(
                    f"{SIM_BASE}/auth/login",
                    json={"email": SIM_USER, "password": SIM_PASSWORD},
                    timeout=5
                )
                r.raise_for_status()
                token = r.json()["token"]
                print("[API] Logged in to simulator.")
                return
            except Exception as e:
                wait = 2 ** attempt
                print(f"[API LOGIN] attempt {attempt+1} failed: {e}; retry in {wait}s")
                time.sleep(wait)
        raise RuntimeError("Simulator login failed after 3 attempts")


def sim_request(method, path, **kwargs):
    global token
    if not token:
        sim_login()

    headers = kwargs.pop("headers", {})
    headers["Authorization"] = f"Bearer {token}"

    for attempt in range(3):
        try:
            r = requests.request(method, f"{SIM_BASE}{path}", headers=headers, timeout=8, **kwargs)
            if r.status_code == 401:
                sim_login()
                headers["Authorization"] = f"Bearer {token}"
                continue
            if r.status_code >= 500:
                wait = 2 ** attempt
                print(f"[API 5xx] {method} {path} -> {r.status_code}; retry in {wait}s")
                time.sleep(wait)
                continue
            if r.status_code >= 400:
                print(f"[API ERROR] {method} {path} -> {r.status_code} {r.text}")
                r.raise_for_status()
            return r
        except requests.exceptions.RequestException as e:
            wait = 2 ** attempt
            print(f"[API RETRY] {method} {path} failed: {e}; retry in {wait}s")
            time.sleep(wait)
    raise RuntimeError(f"API call failed after retries: {method} {path}")


def safe_sync_state(reason="manual"):
    try:
        sync_state()
        log_decision("", "SYNC_OK", f"sync triggered by {reason}")
    except Exception as e:
        log_decision("", "SYNC_ERROR", str(e))


# =====================================================================
# STATE SYNC
# =====================================================================
def detected_count(value):
    if isinstance(value, int):
        return value
    if isinstance(value, list):
        return len(value)
    try:
        return int(value or 0)
    except Exception:
        return 0


def resolve_gate_names(barrier_data):
    """Build gate topology only from /list-barriers.

    The API schema exposes name + zoneParent, but no explicit entry/exit role.
    Therefore PARKMIND never guesses a literal gate name. It classifies:
      - perimeter_gates: zoneParent is empty
      - zone_gates[ZONE]: barriers returned inside that zone

    ENTRY_GATE/EXIT_GATE remain compatibility aliases for the older Level-1
    pipeline. They are chosen deterministically from the API-returned perimeter
    set, never from hardcoded names. Route helpers open the whole API-derived
    path set so correctness does not depend on those aliases being semantic.
    """
    global ENTRY_GATE, EXIT_GATE, perimeter_gates, zone_gates

    names = [str(g.get("name")) for g in barrier_data if g.get("name")]
    if not names:
        raise RuntimeError("Simulator returned no barriers from /list-barriers")

    perimeter = []
    by_zone = defaultdict(list)
    for g in barrier_data:
        name = str(g.get("name") or "").strip()
        if not name:
            continue
        zone = str(g.get("zoneParent") or "").strip()
        if zone:
            by_zone[zone].append(name)
        else:
            perimeter.append(name)

    perimeter_gates = list(perimeter)
    zone_gates = defaultdict(list, {z: list(v) for z, v in by_zone.items()})

    # Compatibility aliases are API-derived. Prefer perimeter barriers because
    # EntrySpot nodes returned by the simulator are also outside zones.
    candidate_pool = perimeter if perimeter else names
    ENTRY_GATE = candidate_pool[0] if candidate_pool else None

    distinct = [n for n in candidate_pool if n != ENTRY_GATE]
    if not distinct:
        distinct = [n for n in names if n != ENTRY_GATE]
    EXIT_GATE = distinct[-1] if distinct else ENTRY_GATE

    log_decision(
        "", "GATE_TOPOLOGY",
        f"perimeter={perimeter_gates}; zone_gates={dict(zone_gates)}; aliases=({ENTRY_GATE},{EXIT_GATE})",
        "All barrier identifiers and zone relationships came from /list-barriers."
    )


def _api_list(path, required=True):
    """One-shot discovery helper. No periodic polling is performed."""
    try:
        value = sim_request("GET", path).json()
        return value if isinstance(value, list) else []
    except Exception:
        if required:
            raise
        return []


def sync_state():
    """Discover the current level from simulator APIs.

    Per the simulator specification this is used on level load/manual recovery,
    not as a polling loop. After sync, webhooks + successful commands maintain
    the in-memory live state used by the dashboard.
    """
    global last_sync_at, exit_lane_plate, api_exit_unknown_count, escape_corridor_plate, api_escape_unknown_count

    park_data = _api_list("/list-parking-spots", required=True)
    barrier_data = _api_list("/list-barriers", required=True)
    light_data = _api_list("/list-lights", required=False)
    fan_data = _api_list("/list-exhaust-fans", required=False)
    alarm_data = _api_list("/list-alarms", required=False)
    zone_data = _api_list("/list-zones", required=False)

    with state_lock:
        prev_reservations = dict(reserved_spots)
        prev_spots = {n: dict(v) for n, v in spots.items()}
        prev_gates = {n: dict(v) for n, v in gates.items()}
        prev_lights = {n: dict(v) for n, v in lights.items()}
        prev_fans = {n: dict(v) for n, v in fans.items()}

        parking_nodes.clear()
        spots.clear()
        entry_spots.clear()
        exit_spots.clear()
        leave_spots.clear()

        for raw in park_data:
            if not raw.get("name"):
                continue
            item = dict(raw)
            name = str(item["name"])
            purpose = str(item.get("purpose") or "")
            parking_nodes[name] = item
            if purpose == "Park":
                spots[name] = {
                    **item,
                    "occupied": detected_count(item.get("detectedCars")) > 0,
                    "usage_count": prev_spots.get(name, {}).get("usage_count", 0),
                }
            elif purpose == "EntrySpot":
                entry_spots[name] = item
            elif purpose == "ExitSpot":
                exit_spots[name] = item
            elif purpose == "LeaveParking":
                leave_spots[name] = item

        reserved_spots.clear()
        for spot_name, res in prev_reservations.items():
            if spot_name in spots and not spots[spot_name]["occupied"]:
                reserved_spots[spot_name] = res

        gates.clear()
        for raw in barrier_data:
            if raw.get("name"):
                name = str(raw["name"])
                gates[name] = {
                    **dict(raw),
                    "usage_count": prev_gates.get(name, {}).get("usage_count", 0),
                }

        resolve_gate_names(barrier_data)

        zones.clear()
        for raw in zone_data:
            if raw.get("name"):
                zones[str(raw["name"])] = dict(raw)

        lights.clear()
        for raw in light_data:
            if raw.get("name"):
                name = str(raw["name"])
                lights[name] = {
                    **dict(raw),
                    "usage_count": prev_lights.get(name, {}).get("usage_count", 0),
                }

        fans.clear()
        for raw in fan_data:
            if raw.get("name"):
                name = str(raw["name"])
                fans[name] = {
                    **dict(raw),
                    "usage_count": prev_fans.get(name, {}).get("usage_count", 0),
                }

        alarms.clear()
        for raw in alarm_data:
            if raw.get("name"):
                alarms[str(raw["name"])] = dict(raw)

        # Reconstruct physical exit occupancy from the API on startup/recovery.
        # This is essential after a controller restart: we must not send a new
        # car toward `exit` while the simulator already has cars at ExitSpot.
        api_exit_plates = []
        api_exit_count = 0
        for exit_name, item in exit_spots.items():
            detected = item.get("detectedCars")
            api_exit_count += detected_count(detected)
            if isinstance(detected, list):
                for value in detected:
                    if isinstance(value, dict):
                        candidate = (value.get("plate") or value.get("name") or
                                     value.get("CarPlateNumber") or value.get("PlateNumber"))
                    else:
                        candidate = value
                    candidate = str(candidate or "").strip()
                    if candidate and candidate not in api_exit_plates:
                        api_exit_plates.append(candidate)
                        exit_spot_by_plate[candidate] = exit_name

        api_exit_unknown_count = max(0, api_exit_count - len(api_exit_plates))
        if api_exit_count > 0:
            for candidate in api_exit_plates:
                if candidate not in physical_exit_queue:
                    physical_exit_queue.append(candidate)
            if exit_lane_plate is None:
                # If the API supplies plate identities, preserve the first one.
                # If it only supplies a count, use a sentinel that blocks any
                # new goto/exit until a recovery sync/event proves Stage A clear.
                exit_lane_plate = api_exit_plates[0] if api_exit_plates else "__API_EXIT_OCCUPIED__"

        # Recover Stage-B occupancy separately from API-discovered LeaveParking
        # nodes. A controller restart must never send a second paid car into an
        # escape corridor that is already occupied.
        api_escape_plates = []
        api_escape_count = 0
        for leave_name, item in leave_spots.items():
            detected = item.get("detectedCars")
            api_escape_count += detected_count(detected)
            if isinstance(detected, list):
                for value in detected:
                    if isinstance(value, dict):
                        candidate = (value.get("plate") or value.get("name") or
                                     value.get("CarPlateNumber") or value.get("PlateNumber"))
                    else:
                        candidate = value
                    candidate = str(candidate or "").strip()
                    if candidate and candidate not in api_escape_plates:
                        api_escape_plates.append(candidate)

        api_escape_unknown_count = max(0, api_escape_count - len(api_escape_plates))
        if api_escape_count > 0 and escape_corridor_plate is None:
            escape_corridor_plate = api_escape_plates[0] if api_escape_plates else "__API_ESCAPE_OCCUPIED__"

        last_sync_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if api_exit_count > 0:
        log_decision(
            "", "EXIT_OCCUPANCY_RECOVERED",
            f"Simulator reports {api_exit_count} car(s) already at ExitSpot; plates={api_exit_plates or 'not supplied'}",
            "Stage-A dispatches are blocked until the existing ExitSpot traffic clears."
        )
    if api_escape_count > 0:
        log_decision(
            "", "ESCAPE_OCCUPANCY_RECOVERED",
            f"Simulator reports {api_escape_count} car(s) at LeaveParking; plates={api_escape_plates or 'not supplied'}",
            "Stage-B escape release is blocked until the existing escape traffic clears."
        )

    print(
        f"[SYNC] {len(spots)} park spots, {len(entry_spots)} entries, "
        f"{len(exit_spots)} exits, {len(gates)} gates, {len(zones)} zones, "
        f"{len(lights)} lights, {len(fans)} fans, {len(alarms)} alarms."
    )
    log_decision(
        "", "SYNC",
        f"API discovery loaded spots={len(spots)}, gates={len(gates)}, zones={len(zones)}, "
        f"lights={len(lights)}, fans={len(fans)}, alarms={len(alarms)}",
        "Discovery GETs are used only at level load/recovery; dashboard uses cached webhook state."
    )


# =====================================================================
# COMPONENT HEALTH (INNOVATION #15)
# =====================================================================
def component_health_score(name, kind):
    """0–100 derived health from API failure flags + locally observed cycles."""
    source = {
        "spot": spots,
        "gate": gates,
        "fan": fans,
        "light": lights,
    }.get(kind, {})
    item = source.get(name, {})

    if item.get("broken") or name in alarms:
        return 0
    if item.get("isUnderMaintenance"):
        return 25
    uses = int(item.get("usage_count") or 0)
    return max(0, 100 - (uses * 100 // HEALTH_SCORE_THRESHOLD_USES))


def _component_zone(name):
    for source in (spots, gates, fans, lights, parking_nodes):
        if name in source:
            return str(source[name].get("zoneParent") or "")
    return ""


def _route_gate_names_for_zone(zone_name, perimeter_role=None):
    """Return only the API-discovered barriers needed for this movement.

    perimeter_role:
      - "entry": API-derived ENTRY_GATE + target-zone barriers
      - "exit":  source-zone barriers + API-derived EXIT_GATE
      - None:    target/source-zone barriers only

    This avoids the old behavior of opening *every* perimeter gate for every car.
    """
    names = []
    if perimeter_role == "entry" and ENTRY_GATE:
        names.append(ENTRY_GATE)
    for n in list(zone_gates.get(zone_name or "", [])):
        if n and n not in names:
            names.append(n)
    if perimeter_role == "exit" and EXIT_GATE and EXIT_GATE not in names:
        names.append(EXIT_GATE)
    return names


def open_route_gates(zone_name, reason="route", perimeter_role=None):
    opened = []
    for name in _route_gate_names_for_zone(zone_name, perimeter_role=perimeter_role):
        try:
            if gate_safe(name):
                state = str(gates.get(name, {}).get("state") or "")
                if state not in ("Open", "Opening"):
                    open_gate(name)
                opened.append(name)
        except Exception as e:
            log_decision("", "ROUTE_GATE_OPEN_ERROR", f"{name}: {e}", reason)
    return opened


def close_zone_route_gates(zone_name, include_perimeter=False):
    names = list(zone_gates.get(zone_name or "", []))
    if include_perimeter:
        names += list(perimeter_gates)
    for name in dict.fromkeys(names):
        try:
            if str(gates.get(name, {}).get("state") or "") in ("Open", "Opening"):
                close_gate(name)
        except Exception:
            pass


def _set_local_component_state(kind, name, on):
    source = fans if kind == "fan" else lights
    item = source.get(name)
    if not item:
        return
    previous = bool(item.get("isOn"))
    item["isOn"] = bool(on)
    if previous != bool(on):
        item["usage_count"] = int(item.get("usage_count") or 0) + 1


def set_fan(name, on, reason="AUTO"):
    if name not in fans:
        raise KeyError(f"Unknown fan from API discovery: {name}")
    fan = fans[name]
    if on and (fan.get("broken") or fan.get("isUnderMaintenance")):
        log_decision("", "FAN_BLOCKED", name, "Broken/maintenance fan cannot be switched on.")
        return False
    action = "on" if on else "off"
    sim_request("POST", f"/exhaust-fans/{quote(name, safe='')}/{action}")
    with state_lock:
        _set_local_component_state("fan", name, on)
    log_decision("", f"FAN_{action.upper()}", name, reason)
    return True


def set_light(name, on, reason="AUTO"):
    if name not in lights:
        raise KeyError(f"Unknown light from API discovery: {name}")
    action = "on" if on else "off"
    sim_request("POST", f"/lights/{quote(name, safe='')}/{action}")
    with state_lock:
        _set_local_component_state("light", name, on)
    log_decision("", f"LIGHT_{action.upper()}", name, reason)
    return True


def set_light_group(group, on, reason="AUTO"):
    group = str(group or "")
    if not group or not any(str(v.get("group") or "") == group for v in lights.values()):
        raise KeyError(f"Unknown light group from API discovery: {group}")
    action = "on" if on else "off"
    sim_request("POST", f"/lights/group/{quote(group, safe='')}/{action}")
    with state_lock:
        for name, item in lights.items():
            if str(item.get("group") or "") == group:
                _set_local_component_state("light", name, on)
    log_decision("", f"LIGHT_GROUP_{action.upper()}", group, reason)
    return True


def repair_component(name):
    """Use only repair endpoints present in the supplied simulator API."""
    if name in gates:
        path = f"/barrier-gates/{quote(name, safe='')}/repair"
        source = gates
        kind = "gate"
    elif name in fans:
        path = f"/exhaust-fans/{quote(name, safe='')}/repair"
        source = fans
        kind = "fan"
    elif name in spots:
        path = f"/parking-spots/{quote(name, safe='')}/repair"
        source = spots
        kind = "spot"
    else:
        # The supplied API does not document a light-repair endpoint.
        log_decision("", "REPAIR_UNSUPPORTED", str(name),
                     "No documented repair endpoint exists for this discovered component.")
        return False

    sim_request("POST", path)
    with state_lock:
        source[name]["isUnderMaintenance"] = True
        maintenance_requested.add(name)
    log_decision("", "REPAIR_REQUESTED", f"{kind}:{name}", "Command sent through simulator API.")
    return True


def apply_co_policy(zone_name):
    """Ventilation follows the simulator-provided zone risk, not a hardcoded fan map."""
    zone = zones.get(zone_name, {})
    risk = str(zone.get("risk") or "").strip().lower()
    if not risk:
        return
    should_on = risk not in {"safe", "low", "normal", "ok"}
    for name, fan in list(fans.items()):
        if str(fan.get("zoneParent") or "") != str(zone_name or ""):
            continue
        override = manual_overrides["fan"].get(name)
        desired = bool(override) if override is not None else should_on
        if bool(fan.get("isOn")) != desired:
            try:
                set_fan(name, desired, f"CO policy: zone={zone_name}, risk={zone.get('risk')}")
            except Exception as e:
                log_decision("", "CO_FAN_ERROR", f"{name}: {e}")


def _parse_sim_time(server_time):
    """Parse simulator timestamps without discarding fractional seconds.

    The earlier build truncated timestamps to whole seconds. Around an exact
    minute boundary that can under-bill by one minute compared with the
    simulator's own higher-precision clock.
    """
    if not server_time:
        return None
    raw = str(server_time).strip()
    # Python accepts both "YYYY-mm-dd HH:MM:SS(.fff)" and ISO "T" forms.
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        pass
    # Defensive fallback for simulator builds that send extra suffix text.
    raw = raw.replace("T", " ")[:19]
    try:
        return datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def simulator_now():
    """Estimate current simulator clock from the latest webhook timestamp.

    We do not poll the simulator. Each webhook re-anchors this clock; between
    webhooks it advances from that API-provided timestamp using monotonic time.
    """
    dt = _parse_sim_time(last_simulator_time)
    if not dt:
        return None
    if last_simulator_time_seen_at is None:
        return dt
    elapsed = max(0.0, time.monotonic() - float(last_simulator_time_seen_at))
    return dt + timedelta(seconds=elapsed)


def apply_light_policy(server_time=None):
    """Morning/day = OFF, night = ON, based on simulator time.

    Default day window is 06:00 <= time < 18:00. Manual overrides still win;
    putting a light/group back to AUTO returns it to this policy.
    """
    dt = _parse_sim_time(server_time) if server_time else simulator_now()
    if not dt:
        return
    should_on = not (DAY_START_HOUR <= dt.hour < DAY_END_HOUR)

    groups = defaultdict(list)
    for name, item in lights.items():
        groups[str(item.get("group") or "")].append(name)

    for group, names in groups.items():
        # Group command is efficient only if no component in the group is manually overridden.
        group_override = manual_overrides["light_group"].get(group)
        individual_override_present = any(n in manual_overrides["light"] for n in names)
        if group and group_override is None and not individual_override_present:
            if any(bool(lights[n].get("isOn")) != should_on for n in names):
                try:
                    set_light_group(group, should_on,
                                    f"Simulator time {dt.strftime('%H:%M:%S')} day/night policy")
                except Exception as e:
                    log_decision("", "LIGHT_POLICY_ERROR", f"group={group}: {e}")
            continue

        for name in names:
            desired = should_on
            if group_override is not None:
                desired = bool(group_override)
            if name in manual_overrides["light"]:
                desired = bool(manual_overrides["light"][name])
            if bool(lights[name].get("isOn")) != desired:
                try:
                    set_light(name, desired,
                              f"Simulator time {dt.strftime('%H:%M:%S')} day/night/manual policy")
                except Exception as e:
                    log_decision("", "LIGHT_POLICY_ERROR", f"{name}: {e}")


def light_policy_watchdog():
    """Enforce day/night transitions without simulator polling.

    The clock remains anchored to the latest ServerDateTime received via webhook.
    This makes lights switch at 06:00/18:00 even if no unrelated webhook happens
    at the exact boundary.
    """
    while True:
        time.sleep(2)
        try:
            if lights and last_simulator_time:
                apply_light_policy()
        except Exception as e:
            print("[LIGHT WATCHDOG]", e)


def maintenance_watchdog():
    """Cache-only preventive/corrective maintenance; never polls discovery APIs."""
    while True:
        time.sleep(5)
        try:
            candidates = []
            with state_lock:
                for kind, source in (("spot", spots), ("gate", gates), ("fan", fans)):
                    for name, item in source.items():
                        if name in maintenance_requested or item.get("isUnderMaintenance"):
                            continue
                        needs = bool(item.get("broken")) or name in alarms
                        preventive = component_health_score(name, kind) <= PREVENTIVE_MAINTENANCE_HEALTH
                        if needs or preventive:
                            candidates.append(name)
            for name in candidates:
                try:
                    repair_component(name)
                except Exception as e:
                    log_decision("", "AUTO_REPAIR_ERROR", f"{name}: {e}")
        except Exception as e:
            print("[MAINTENANCE WATCHDOG]", e)


# =====================================================================
# GATE HELPERS
# =====================================================================
def gate_safe(name):
    g = gates.get(name)
    if not g:
        return True
    return not g.get("broken", False) and not g.get("isUnderMaintenance", False)


def gate_manual_override(name):
    """Return True=hold OPEN, False=hold CLOSED, None=AUTO."""
    with state_lock:
        return manual_overrides.get("gate", {}).get(name)


def open_gate(name, manual=False):
    # A dashboard CLOSE override is persistent: automation may not reopen it.
    # Explicit dashboard/manual commands pass manual=True and may change it.
    override = gate_manual_override(name)
    if override is False and not manual:
        log_decision(
            "", "GATE_OPEN_BLOCKED_BY_OVERRIDE", name,
            "Operator selected MANUAL CLOSED; automatic open command suppressed until AUTO or OPEN is selected."
        )
        return False

    if not gate_safe(name):
        log_decision("", "BLOCKED", f"Refused to open {name}: broken/maintenance",
                     "Gate safety check failed.")
        return False
    sim_request("POST", f"/barrier-gates/{quote(name, safe='')}/open")
    with state_lock:
        if name in gates:
            gates[name]["state"] = "Opening"
    log_decision("", "GATE_OPEN_CMD", name,
                 "Command sent through authenticated simulator API; webhook remains authoritative.")
    return True


def close_gate(name, force=False, manual=False):
    # HARD INVARIANT: the API-discovered entrance barrier is part of the
    # simulator's A -> EntrySpot spawn path. Closing it prevents the simulator
    # from creating the next vehicle, so it cannot be manually or automatically
    # held closed during a live run.
    if ENTRY_GATE and name == ENTRY_GATE:
        log_decision(
            "", "ENTRY_GATE_CLOSE_BLOCKED", name,
            "Spawn-route safety invariant: entrance barrier must remain open so A -> EntrySpot always has a path."
        )
        return False

    # A dashboard OPEN override is persistent. Even force=True is only for
    # bypassing normal keep-open policy; it must NOT bypass an operator override.
    override = gate_manual_override(name)
    if override is True and not manual:
        log_decision(
            "", "GATE_CLOSE_BLOCKED_BY_OVERRIDE", name,
            "Operator selected MANUAL OPEN; automatic close command suppressed until AUTO or CLOSE is selected."
        )
        return False

    # Other configured keep-open barriers are protected from automatic cleanup.
    if not force and name in keep_open_gate_names():
        return False
    if not gate_safe(name):
        log_decision("", "BLOCKED", f"Refused to close {name}: broken/maintenance",
                     "Gate safety check failed.")
        return False
    sim_request("POST", f"/barrier-gates/{quote(name, safe='')}/close")
    with state_lock:
        if name in gates:
            gates[name]["state"] = "Closing"
    log_decision("", "GATE_CLOSE_CMD", name,
                 "Command sent through authenticated simulator API; webhook remains authoritative.")
    return True


manual_gate_closed = set()      # keep-open gates an operator closed on purpose
_keep_open_last_try = {}        # name -> monotonic time of the last auto-open attempt


def keep_open_gate_names():
    """Barriers that stay open while idle. The discovered entry gate is mandatory."""
    names = {n for n in KEEP_OPEN_GATES_ENV if n in gates}
    if ENTRY_GATE and ENTRY_GATE in gates:
        names.add(ENTRY_GATE)
    return names


def ensure_keep_open_gates():
    """Open keep-open barriers that are not already Open/Opening (debounced)."""
    for name in keep_open_gate_names():
        # ENTRY_GATE can never be held closed because of the spawn-route invariant.
        # Other keep-open gates may be deliberately held CLOSED by an operator.
        if ((name in manual_gate_closed or gate_manual_override(name) is False)
                and name != ENTRY_GATE):
            continue
        if not gate_safe(name):
            continue
        state = str(gates.get(name, {}).get("state") or "")
        if state in ("Open", "Opening"):
            continue
        now = time.monotonic()
        if now - _keep_open_last_try.get(name, 0.0) < 3.0:
            continue
        _keep_open_last_try[name] = now
        try:
            open_gate(name)
            log_decision("", "KEEP_OPEN", name,
                         "Entry-side barrier held open while idle so arriving cars always have a route.")
        except Exception as e:
            log_decision("", "KEEP_OPEN_ERROR", f"{name}: {e}")


def mark_entry_dispatched(plate, spot):
    """Record that a vehicle has received a successful route command."""
    now = time.time()
    with state_lock:
        if entry_active and entry_active.get("plate") == plate:
            entry_active["sent"] = True
            entry_active["sent_at"] = now

        current = in_transit.get(plate, {})
        in_transit[plate] = {
            "spot": spot,
            "sent_at": now,
            "route_retries": int(current.get("route_retries") or 0),
        }


def close_idle_gates():
    """Close API-discovered perimeter gates when no vehicle movement needs them."""
    while True:
        time.sleep(0.5)
        try:
            with state_lock:
                movement_idle = (
                    entry_active is None and not entry_queue and not in_transit
                    and exit_active is None and exit_lane_plate is None and not physical_exit_queue
                )
                candidates = list(gates.keys())
            keep_open = keep_open_gate_names()
            if movement_idle:
                for name in candidates:
                    if name in keep_open:
                        continue
                    if str(gates.get(name, {}).get("state") or "") in ("Open", "Opening"):
                        close_gate(name)
            ensure_keep_open_gates()
        except Exception as e:
            print("[IDLE GATE WATCHDOG]", e)


def gate_confirmed_open(name):
    """True when a real gate webhook has reported Opening/Open."""
    state = str(gates.get(name, {}).get("state") or "")
    return state in ("Opening", "Open")


def gate_fully_open(name):
    """Strict movement permission: the car moves only after webhook state == Open."""
    return str(gates.get(name, {}).get("state") or "") == "Open"


def complete_zone_entry_crossing(plate, zone_name):
    """Release a zone-gate slot shortly after dispatch, never after parking.

    The simulator API does not expose a dedicated "car crossed barrier" webhook.
    After the simulator confirms EntrySpot CarOut, a short configurable clearance
    window is the safest API-only signal that the car has moved beyond the internal
    gate path. This prevents a queue at one zone without forcing
    the next arrival to wait for the first car to reach its parking bay.
    """
    zone_name = str(zone_name or "")
    if not zone_name:
        return

    released = False
    with state_lock:
        if zone_entry_active.get(zone_name) == plate:
            zone_entry_active.pop(zone_name, None)
            released = True

    if not released:
        return

    log_decision(
        plate, "ZONE_GATE_CLEARED", zone_name,
        f"Released zone admission slot {ZONE_GATE_CLEARANCE_SEC:.2f}s after EntrySpot CarOut; parking completion is not required."
    )
    close_zone_if_idle(zone_name)
    # A waiting entry can now reconsider Zone 1/2/3 immediately.
    timer = threading.Timer(0.05, process_entry_queue)
    timer.daemon = True
    timer.start()


def try_dispatch_entry_when_ready(plate):
    """Send one admitted car only after every required barrier is fully Open."""
    with state_lock:
        active = dict(entry_active or {})
        if not active or active.get("plate") != plate or active.get("sent"):
            return False
        required = list(active.get("required_gates") or [])
        spot = active.get("spot")
        target_zone = str(active.get("zone") or "")

    if required and not all(gate_fully_open(name) for name in required):
        return False

    try:
        send_car(plate, spot)
        mark_entry_dispatched(plate, spot)
        log_decision(
            plate, "ENTRY_JIT_RELEASE",
            f"All required barriers OPEN; routed to {spot}",
            f"required_gates={required}. Zone slot will be released after gate clearance, not after parking."
        )

        # Do NOT release the zone slot here. The car has merely received its
        # goto command. We wait for the simulator's EntrySpot CarOut webhook,
        # then apply a short gate-clearance window.
        return True
    except Exception as e:
        log_decision(plate, "ENTRY_JIT_RELEASE_ERROR", str(e), f"required_gates={required}")
        return False


def zone_crossing_busy(zone_name, excluding_plate=None):
    """True if another car currently owns this zone's barrier crossing."""
    zone_name = str(zone_name or "")
    if not zone_name:
        return False
    with state_lock:
        inbound_owner = zone_entry_active.get(zone_name)
        if inbound_owner and inbound_owner != excluding_plate:
            return True
        for p, z in exit_route_zone.items():
            if p != excluding_plate and str(z or "") == zone_name:
                return True
    return False


def close_entry_if_idle():
    """Close only NON-entry perimeter barriers when the entry pipeline is idle."""
    with state_lock:
        if entry_active is not None or entry_queue:
            return
        names = [name for name in perimeter_gates if name != ENTRY_GATE]
    for name in names:
        try:
            close_gate(name)
        except Exception as e:
            log_decision("", "ENTRY_IDLE_CLOSE_ERROR", f"{name}: {e}")
    # Reassert the spawn-route invariant after any perimeter cleanup.
    ensure_keep_open_gates()


def send_car(plate, destination):
    """Send a simulator car to a parking spot / exit / leavepark."""
    plate_path = quote(str(plate).replace(" ", ""), safe="")
    dest_path = quote(str(destination), safe="")
    sim_request("POST", f"/car/{plate_path}/goto/{dest_path}")
    log_decision(plate, "CAR_GOTO", destination)


def charge_car(plate, parking_cost, charging_cost):
    """Request the simulator to charge a car at the exit."""
    plate_path = quote(str(plate).replace(" ", ""), safe="")
    sim_request(
        "POST",
        f"/car/{plate_path}/charge",
        params={
            "parkingCost": parking_cost,
            "chargingCost": charging_cost
        }
    )
    log_decision(
        plate,
        "CHARGE",
        f"parking={parking_cost}, charging={charging_cost}"
    )


def delayed_entry_release(plate, spot, attempt=1):
    """
    Fallback route command if gate_action webhooks are late/missing.

    send_car() already has its own transient HTTP retries. If that whole call
    still fails, this layer retries the routing operation up to 4 times with
    increasing delay. sent=True is written ONLY after a successful API call.
    """
    global entry_active

    with state_lock:
        if not entry_active:
            return
        if entry_active.get("plate") != plate or entry_active.get("spot") != spot:
            return
        if entry_active.get("sent"):
            return

    try:
        send_car(plate, spot)
        mark_entry_dispatched(plate, spot)

        log_decision(
            plate,
            "ENTRY_RELEASE_FALLBACK",
            f"Routing to {spot} succeeded on fallback attempt {attempt}.",
            "Fallback protects against late gate webhooks and transient API failure."
        )

    except Exception as e:
        log_decision(
            plate,
            "ENTRY_RELEASE_ERROR",
            f"attempt {attempt}/4: {e}",
            "sent flag NOT set; routing fallback remains retryable."
        )

        if attempt < 4:
            retry = threading.Timer(
                2.0 * attempt,
                delayed_entry_release,
                args=(plate, spot, attempt + 1)
            )
            retry.daemon = True
            retry.start()
        else:
            log_decision(
                plate,
                "ENTRY_RELEASE_ABANDONED",
                "Routing failed 4 fallback attempts; operator intervention required.",
                "Fail-safe: entry_active is retained so another vehicle is not released into a conflicting movement."
            )


def delayed_exit_release(plate, attempt=1):
    """Paid-only fallback; the hard payment interlock is rechecked every time."""
    global exit_active

    authorized, why = payment_release_authorized(plate, require_exit_owner=True)
    if not authorized:
        log_decision(
            plate, "EXIT_FALLBACK_PAYMENT_BLOCK", why,
            "Fallback refused: NO PAYMENT = NO LEAVE."
        )
        return

    with state_lock:
        if not exit_active or exit_active.get("plate") != plate:
            return
        if exit_active.get("sent"):
            return

    try:
        send_car(plate, "leavepark")
        with state_lock:
            if exit_active and exit_active.get("plate") == plate:
                exit_active["sent"] = True

        log_decision(
            plate,
            "EXIT_RELEASE_FALLBACK",
            f"Paid vehicle release succeeded on fallback attempt {attempt}.",
            "Fallback protects against late exit-gate webhooks and transient API failure."
        )

    except Exception as e:
        log_decision(
            plate,
            "EXIT_RELEASE_ERROR",
            f"attempt {attempt}/4: {e}",
            "sent flag NOT set; exit fallback remains retryable."
        )

        if attempt < 4:
            retry = threading.Timer(
                2.0 * attempt,
                delayed_exit_release,
                args=(plate, attempt + 1)
            )
            retry.daemon = True
            retry.start()
        else:
            log_decision(
                plate,
                "EXIT_RELEASE_ABANDONED",
                "leavepark failed 4 fallback attempts; operator intervention required.",
                "Fail-safe: exit_active remains held to avoid releasing another car into the same exit movement."
            )


def entry_gate_open_watchdog(plate, spot, attempt=1):
    """
    Retry entrance OPEN only if the simulator has NOT confirmed Opening/Open.
    This avoids hammering the gate while its animation is already in progress.
    """
    global entry_active

    with state_lock:
        if not entry_active:
            return
        if entry_active.get("plate") != plate or entry_active.get("spot") != spot:
            return
        if entry_active.get("sent"):
            return

    # If a real gate event says it is opening/open, do not restart the command.
    if gate_confirmed_open(ENTRY_GATE):
        return

    if attempt > 4:
        log_decision(
            plate, "ENTRY_GATE_TIMEOUT",
            f"{ENTRY_GATE} did not confirm opening after 4 checks.",
            "Routing fallback remains available; gate command is not spammed."
        )
        return

    try:
        open_gate(ENTRY_GATE)
        log_decision(
            plate, "ENTRY_GATE_RETRY",
            f"{ENTRY_GATE} open retry {attempt}/4",
            "Retry issued only because no Opening/Open confirmation was received."
        )
    except Exception as e:
        log_decision(plate, "ENTRY_GATE_OPEN_ERROR", str(e))

    timer = threading.Timer(
        1.0, entry_gate_open_watchdog, args=(plate, spot, attempt + 1)
    )
    timer.daemon = True
    timer.start()


def exit_gate_open_watchdog(plate, attempt=1):
    global exit_active

    with state_lock:
        if not exit_active or exit_active.get("plate") != plate:
            return
        if exit_active.get("sent"):
            return

    if gate_confirmed_open(EXIT_GATE):
        return

    if attempt > 4:
        log_decision(
            plate, "EXIT_GATE_TIMEOUT",
            f"{EXIT_GATE} did not confirm opening after 4 checks.",
            "Paid vehicle remains safely held; gate command is not spammed."
        )
        return

    try:
        open_gate(EXIT_GATE)
        log_decision(
            plate, "EXIT_GATE_RETRY",
            f"{EXIT_GATE} open retry {attempt}/4",
            "Retry issued only because no Opening/Open confirmation was received."
        )
    except Exception as e:
        log_decision(plate, "EXIT_GATE_OPEN_ERROR", str(e))

    timer = threading.Timer(
        1.0, exit_gate_open_watchdog, args=(plate, attempt + 1)
    )
    timer.daemon = True
    timer.start()



# =====================================================================
# [INNOVATION #5] — SMART SPOT SELECTION (5-FACTOR SCORING)
# =====================================================================
def natural_spot_key(name):
    m = re.search(r"(\d+)$", name)
    return int(m.group(1)) if m else 999999


def compatible(spot, car_type):
    target = str(spot.get("parkingForCarType", "Any")).lower()
    car = str(car_type or "Normal").lower()
    if target == "any":
        return True
    if "electric" in car and target == "electric":
        return True
    if "accessible" in car and target == "accessible":
        return True
    return False


def zone_load(zone_name):
    """0..1 — live zone pressure including in-flight reservations.

    Occupied spot = 1.0 busy
    Reserved but not yet occupied = 0.5 busy
    Free = 0.0
    """
    total = 0
    busy_weight = 0.0
    for name, s in spots.items():
        if s.get("zoneParent") == zone_name:
            total += 1
            if s.get("occupied"):
                busy_weight += 1.0
            elif name in reserved_spots:
                busy_weight += 0.5
    if total == 0:
        return 0
    return busy_weight / total


def proximity_to_exit_score(spot_name):
    """
    Heuristic: spots with smaller numeric suffix are closer to the
    exit (you can refine this once you observe the simulator layout).
    Used to send short-term parkers near the exit, long-term parkers
    deeper in.
    """
    n = natural_spot_key(spot_name)
    return 1.0 - min(n / 500.0, 1.0)  # smaller index = higher score


def smart_spot_score(spot_name, spot, car_type, planned_minutes):
    """Compute a 0..1 score. Higher is better."""
    score = 0.0
    reasons = []

    # Factor 1: compatibility (gate)
    if not compatible(spot, car_type):
        return -1.0, "INCOMPATIBLE"
    score += 0.25
    reasons.append("COMPATIBLE=✓")

    # Factor 2: zone load balancing — prefer zones that are less full.
    zone = spot.get("zoneParent", "")
    load = zone_load(zone)
    balance_score = 1.0 - load
    score += 0.20 * balance_score
    reasons.append(f"ZONE_BALANCE={balance_score:.2f}")

    # Factor 3: spot health
    health = component_health_score(spot_name, "spot") / 100.0
    score += 0.20 * health
    reasons.append(f"HEALTH={int(health*100)}")

    # Factor 4: rotation — favor spots with fewer past uses (distributes wear).
    uses = spot.get("usage_count", 0)
    rot_score = 1.0 - min(uses / 20.0, 1.0)
    score += 0.15 * rot_score
    reasons.append(f"ROTATION={rot_score:.2f}")

    # Factor 5: proximity-to-exit. Short-term parkers get exit-adjacent spots;
    # long-term parkers can be deeper.
    prox = proximity_to_exit_score(spot_name)
    if planned_minutes and planned_minutes <= 3:
        score += 0.20 * prox
        reasons.append(f"PROXIMITY(short-term)={prox:.2f}")
    else:
        score += 0.20 * (1.0 - prox)  # prefer deeper
        reasons.append(f"PROXIMITY(long-term)={1-prox:.2f}")

    return score, " | ".join(reasons)


def _natural_zone_key(name):
    text = str(name or "")
    m = re.search(r"(\d+)$", text)
    return (int(m.group(1)) if m else 999999, text.lower())


def choose_spot(car_type, planned_minutes, avoid_busy_zone_gates=False):
    """Choose a safe compatible bay while balancing cars across zones.

    LEVEL-2 ADMISSION POLICY:
      * Only healthy, free, compatible and unreserved spots are candidates.
      * Prefer the zone with the FEWEST cars already committed to it.
        A committed car means either physically occupied or currently reserved /
        in transit to a bay.  Natural zone order breaks ties.
      * Example with Zone1/Zone2/Zone3 counts:
          0/0/0 -> Zone1
          1/0/0 -> Zone2
          1/1/0 -> Zone3
          1/1/1 -> Zone1
      * If a zone gate path is currently busy, skip that zone for this admission
        so a second car is never stacked behind the same internal barrier.
      * Unzoned spots remain a last-resort fallback only.

    This changes ONLY destination selection.  The existing reservation, gate,
    dispatch, payment and exit state machines remain unchanged.
    """

    def committed_count(zone_name):
        """Cars already occupying or reserved/in-transit to this zone."""
        total = 0
        for spot_name, spot in spots.items():
            if str(spot.get("zoneParent") or "") != zone_name:
                continue
            if spot.get("occupied"):
                total += 1
            elif spot_name in reserved_spots:
                total += 1
        return total

    zone_names = sorted(
        {
            str(s.get("zoneParent") or "")
            for s in spots.values()
            if str(s.get("zoneParent") or "")
        },
        key=_natural_zone_key,
    )

    usable_zones = []
    busy_zones_with_capacity = []

    # Build safe/compatible candidates per zone first.  We intentionally do not
    # pick Zone1 merely because it appears first; zone occupancy decides priority.
    for zone_name in zone_names:
        candidates = []
        for name, spot in spots.items():
            if str(spot.get("zoneParent") or "") != zone_name:
                continue
            if spot.get("occupied") or spot.get("broken") or spot.get("isUnderMaintenance"):
                continue
            if name in reserved_spots:
                continue

            score, reason = smart_spot_score(name, spot, car_type, planned_minutes)
            if score < 0:
                continue
            candidates.append((score, name, reason))

        if not candidates:
            continue

        if avoid_busy_zone_gates and zone_crossing_busy(zone_name):
            busy_zones_with_capacity.append(zone_name)
            continue

        candidates.sort(key=lambda x: (-x[0], natural_spot_key(x[1]), x[1]))
        usable_zones.append((committed_count(zone_name), _natural_zone_key(zone_name), zone_name, candidates))

    if usable_zones:
        # Lowest committed-car count wins.  Ties go Zone1 -> Zone2 -> Zone3 ...
        usable_zones.sort(key=lambda x: (x[0], x[1]))
        committed, _, zone_name, candidates = usable_zones[0]
        best = candidates[0]
        spill = ""
        if busy_zones_with_capacity:
            spill = f"SPILLED_PAST_BUSY={','.join(busy_zones_with_capacity)} | "
        return (
            best[1],
            f"{spill}ZONE_BALANCE_COUNT={committed} | ZONE_PRIORITY={zone_name} | {best[2]}"
        )

    # Keep unzoned spots strictly as a last fallback for future simulator levels.
    unzoned_candidates = []
    for name, spot in spots.items():
        if str(spot.get("zoneParent") or ""):
            continue
        if spot.get("occupied") or spot.get("broken") or spot.get("isUnderMaintenance"):
            continue
        if name in reserved_spots:
            continue
        score, reason = smart_spot_score(name, spot, car_type, planned_minutes)
        if score >= 0:
            unzoned_candidates.append((score, name, reason))

    if unzoned_candidates:
        unzoned_candidates.sort(key=lambda x: (-x[0], natural_spot_key(x[1]), x[1]))
        best = unzoned_candidates[0]
        return best[1], f"ZONE_PRIORITY=UNZONED | {best[2]}"

    if avoid_busy_zone_gates and busy_zones_with_capacity:
        return None, f"WAIT_ALL_ZONE_GATES_BUSY={','.join(busy_zones_with_capacity)}"

    return None, ""


# =====================================================================
# DESTINATION COMPLIANCE / SELF-HEALING ROUTE RECOVERY
# =====================================================================
def spot_is_safe_for_car(spot_name, plate, car_type):
    """Can this physical spot safely become this car's final spot?"""
    s = spots.get(spot_name)
    if not s:
        return False, "UNKNOWN_SPOT"
    if s.get("broken"):
        return False, "BROKEN"
    if s.get("isUnderMaintenance"):
        return False, "UNDER_MAINTENANCE"
    if not compatible(s, car_type):
        return False, "INCOMPATIBLE_TYPE"

    reservation = reserved_spots.get(spot_name)
    if reservation and reservation.get("plate") != plate:
        return False, f"RESERVED_FOR_{reservation.get('plate')}"

    return True, "SAFE"


def zone_route_busy(zone_name):
    """True only while a vehicle owns this zone's barrier crossing slot.

    Crucially, an in-transit car does NOT keep the zone busy until it parks.
    The inbound slot is released by complete_zone_entry_crossing() shortly
    after successful dispatch through an already-open gate.
    """
    zone_name = str(zone_name or "")
    if not zone_name:
        return False
    with state_lock:
        if zone_entry_active.get(zone_name):
            return True
        if any(str(z or "") == zone_name for z in exit_route_zone.values()):
            return True
    return False


def close_zone_if_idle(zone_name):
    """Close only when no entering/exiting vehicle still needs the zone path."""
    zone_name = str(zone_name or "")
    if zone_name and not zone_route_busy(zone_name):
        close_zone_route_gates(zone_name, include_perimeter=False)


def release_zone_entry_lock(plate):
    """Compatibility helper: release legacy lock and close only if route is idle."""
    zones_to_check = set()
    with state_lock:
        for zone_name, owner in list(zone_entry_active.items()):
            if owner == plate:
                zone_entry_active.pop(zone_name, None)
                zones_to_check.add(zone_name)
        car = get_car(plate) or {}
        spot_name = car.get("actual_spot") or car.get("assigned_spot") or ""
        zone_name = str(spots.get(spot_name, {}).get("zoneParent") or "")
        if zone_name:
            zones_to_check.add(zone_name)
    for zone_name in zones_to_check:
        close_zone_if_idle(zone_name)
    return list(zones_to_check)


def finish_successful_parking(plate, spot_name, server_time, planned):
    """Normal successful parking finalization, shared by assigned and self-healed arrivals."""
    global entry_active

    reserved_spots.pop(spot_name, None)
    car = get_car(plate) or {}
    originally_assigned = car.get("assigned_spot")

    # Release an old reservation if the system safely adopted a different physical spot.
    if originally_assigned and originally_assigned != spot_name:
        old_res = reserved_spots.get(originally_assigned)
        if old_res and old_res.get("plate") == plate:
            reserved_spots.pop(originally_assigned, None)

    upsert_car(
        plate,
        assigned_spot=spot_name,
        actual_spot=spot_name,
        parked_time=server_time,
        status="PARKED"
    )
    append_journey(plate, f"parked@{spot_name}")
    log_decision(
        plate, "PARKED", spot_name,
        "Physical occupancy confirmed; reservation released."
    )

    planned_minutes = ((get_car(plate) or {}).get("planned_minutes") or planned or 1)
    schedule_exit(plate, planned_minutes)

    with state_lock:
        stats["total_parked"] += 1
        in_transit.pop(plate, None)
        if entry_active and entry_active.get("plate") == plate:
            entry_active = None

    # The car has cleared its internal zone barrier path: close the exact
    # API-derived zone path it owned (even if a misroute changed actual_spot).
    release_zone_entry_lock(plate)
    threading.Timer(0.15, process_entry_queue).start()


def handle_wrong_spot(plate, actual_spot, server_time, planned):
    """
    A car physically entered a spot different from the one PARKMIND assigned.

    Recovery order:
      1) If the actual spot is still safe/compatible/uncontested, adopt it.
      2) Otherwise reroute to the original intended spot if it is still safe.
      3) Otherwise choose and reserve a new safe spot.
      4) If nothing safe exists, enter ROUTE_HOLD and stop admission progression.
    """
    global entry_active
    car = get_car(plate) or {}
    expected = car.get("assigned_spot")
    car_type = car.get("car_type") or "Normal"
    mismatch_count = int(car.get("route_mismatch_count") or 0) + 1

    upsert_car(
        plate,
        actual_spot=actual_spot,
        route_mismatch_count=mismatch_count,
        status="MISROUTED"
    )
    append_journey(plate, f"wrong_spot@{actual_spot}")
    log_anomaly(
        plate,
        "WRONG_SPOT",
        f"Expected {expected or 'UNKNOWN'}, physically detected at {actual_spot}"
    )
    log_decision(
        plate, "ROUTE_DEVIATION",
        f"Expected {expected or 'UNKNOWN'} but detected at {actual_spot}",
        "Destination Compliance Shield triggered."
    )

    # Best self-healing outcome: the car is already in a valid, uncontested space.
    actual_ok, actual_reason = spot_is_safe_for_car(actual_spot, plate, car_type)
    if actual_ok:
        log_decision(
            plate, "SELF_HEAL_ACCEPT_SPOT",
            f"Safely adopting {actual_spot} as the final assignment.",
            "Actual spot is healthy, compatible and not reserved for another vehicle."
        )
        finish_successful_parking(plate, actual_spot, server_time, planned)
        return

    # Otherwise try to restore the original assignment.
    if expected and expected != actual_spot:
        intended = spots.get(expected)
        intended_res = reserved_spots.get(expected)
        intended_owner_ok = (not intended_res) or intended_res.get("plate") == plate
        if (intended and not intended.get("occupied")
                and not intended.get("broken")
                and not intended.get("isUnderMaintenance")
                and compatible(intended, car_type)
                and intended_owner_ok):
            log_decision(
                plate, "AUTO_REROUTE",
                f"{actual_spot} -> {expected}",
                f"Actual spot unsafe ({actual_reason}); original reservation still valid."
            )
            send_car(plate, expected)
            upsert_car(plate, status="REROUTING")
            append_journey(plate, f"reroute->{expected}")
            return

    # Original target is no longer safe: reserve a new destination.
    new_spot, reason = choose_spot(car_type, int(car.get("planned_minutes") or planned or 0))
    if new_spot:
        # Release the old reservation belonging to this plate.
        if expected:
            old_res = reserved_spots.get(expected)
            if old_res and old_res.get("plate") == plate:
                reserved_spots.pop(expected, None)

        reserved_spots[new_spot] = {"plate": plate, "reserved_at": time.time()}
        upsert_car(plate, assigned_spot=new_spot, status="REROUTING", decision=reason)
        log_decision(
            plate, "AUTO_REASSIGN",
            f"{actual_spot} -> {new_spot}",
            f"Original target unavailable; new safe target selected. {reason}"
        )
        send_car(plate, new_spot)
        append_journey(plate, f"reassign->{new_spot}")
        return

    # Impossible/safety case: do not blindly move the vehicle.
    # IMPORTANT: a held/misrouted car must NOT permanently own the global
    # entry pipeline. It is physically accounted for by the spot sensor and
    # is now an operator-recovery case.
    upsert_car(plate, status="ROUTE_HOLD")
    log_decision(
        plate, "ROUTE_HOLD",
        f"Vehicle remains at {actual_spot}; no safe reroute exists.",
        "Fail-safe: vehicle is isolated for operator recovery; new arrivals may continue."
    )

    with state_lock:
        # Remove any in-flight reservation owned by this plate. The actual
        # physical spot remains occupied in spots[] and therefore cannot be
        # assigned to another vehicle.
        for reserved_name in list(reserved_spots.keys()):
            res = reserved_spots.get(reserved_name)
            if res and res.get("plate") == plate:
                reserved_spots.pop(reserved_name, None)

        if entry_active and entry_active.get("plate") == plate:
            entry_active = None

        in_transit.pop(plate, None)

    release_zone_entry_lock(plate)
    threading.Timer(1.0, process_entry_queue).start()


# =====================================================================
# PARKING LOGIC
# =====================================================================
def process_entry_queue():
    global entry_active
    with state_lock:
        if entry_active is not None or not entry_queue:
            return
        if not spots:
            try:
                sync_state()
            except Exception as e:
                log_decision("", "ERROR", f"sync failed during entry: {e}")
            if not spots:
                print("[WAIT] Level components are not ready yet; entry remains queued.")
                threading.Timer(1.0, process_entry_queue).start()
                return

        item = entry_queue.pop(0)
        plate = item["plate"]
        car_type = item["car_type"]
        planned = item["planned"]

        if demo_state.get("live_full_next_arrival"):
            demo_state["live_full_next_arrival"] = False
            spot, reason = None, "ONE-SHOT LIVE FULL-LOT TEST"
        else:
            # Admission-aware selection: if one car already occupies Zone 1's
            # gate path, immediately try Zone 2; then Zone 3; etc.
            spot, reason = choose_spot(car_type, planned, avoid_busy_zone_gates=True)

        if not spot:
            if str(reason).startswith("WAIT_ALL_ZONE_GATES_BUSY="):
                # There are safe bays, but every usable zone gate currently has
                # one crossing in progress. Keep this car at the EntrySpot rather
                # than stacking it behind any internal gate.
                entry_queue.insert(0, item)
                upsert_car(plate, status="WAITING_ZONE_GATE", decision=reason)
                log_decision(
                    plate, "ALL_ZONE_GATES_BUSY", reason,
                    "Car remains at EntrySpot. It will be reconsidered as soon as any zone crossing slot clears."
                )
                retry = threading.Timer(0.5, process_entry_queue)
                retry.daemon = True
                retry.start()
                return

            upsert_car(plate, status="LEFT_FULL")
            log_decision(plate, "NO_SPACE", "No safe compatible parking spot. Sending car away.",
                         "All candidates occupied/broken/reserved.")
            try:
                send_car(plate, "leavepark")
            except Exception as e:
                log_decision(plate, "ERROR", f"leavepark failed: {e}")
            threading.Timer(0.2, process_entry_queue).start()
            return

        target_zone = str(spots.get(spot, {}).get("zoneParent") or "")

        # Race-condition guard: the selector already skips busy zones, but a
        # webhook/thread could claim this zone between selection and reservation.
        # Requeue and immediately reconsider the next zone instead of waiting here.
        if target_zone and zone_crossing_busy(target_zone, excluding_plate=plate):
            entry_queue.insert(0, item)
            log_decision(
                plate, "ZONE_GATE_RACE_RETRY",
                f"{target_zone} became busy during assignment; selecting another zone.",
                "Never stack a second vehicle behind an occupied zone gate."
            )
            retry = threading.Timer(0.05, process_entry_queue)
            retry.daemon = True
            retry.start()
            return

        reserved_spots[spot] = {"plate": plate, "reserved_at": time.time()}
        if target_zone:
            zone_entry_active[target_zone] = plate

        entry_active = {
            "plate": plate, "spot": spot, "zone": target_zone,
            "sent": False, "sent_at": None, "route_retries": 0,
            "required_gates": []
        }
        upsert_car(plate, assigned_spot=spot, status="ASSIGNED", decision=reason)
        log_decision(plate, "ASSIGN", spot, reason)

        try:
            route_gates = _route_gate_names_for_zone(target_zone, perimeter_role="entry")
            entry_active["required_gates"] = list(route_gates)
            opened = open_route_gates(target_zone, reason=f"entry->{spot}", perimeter_role="entry")
            log_decision(
                plate, "ENTRY_ROUTE_GATES",
                f"zone={target_zone or 'UNASSIGNED'} gates={opened}",
                "JIT: barriers opened only for this car; dispatch waits for Open webhook."
            )

            try_dispatch_entry_when_ready(plate)

            def jit_gate_watch(attempt=1):
                with state_lock:
                    active = dict(entry_active or {})
                    if not active or active.get("plate") != plate or active.get("sent"):
                        return
                    required = list(active.get("required_gates") or [])
                if try_dispatch_entry_when_ready(plate):
                    return
                if attempt > 8:
                    log_decision(plate, "ENTRY_GATE_HOLD",
                                 f"Still waiting for Open: {required}",
                                 "Car remains at EntrySpot; no closed-gate routing is allowed.")
                    return
                for gate_name in required:
                    if not gate_fully_open(gate_name):
                        try:
                            state = str(gates.get(gate_name, {}).get("state") or "")
                            if state not in ("Opening", "Open"):
                                open_gate(gate_name)
                        except Exception as e:
                            log_decision(plate, "ENTRY_GATE_RETRY_ERROR", f"{gate_name}: {e}")
                timer = threading.Timer(0.5, jit_gate_watch, args=(attempt + 1,))
                timer.daemon = True
                timer.start()

            timer = threading.Timer(0.5, jit_gate_watch)
            timer.daemon = True
            timer.start()

        except Exception as e:
            if entry_active and entry_active.get("plate") == plate:
                entry_active = None
            if target_zone and zone_entry_active.get(target_zone) == plate:
                zone_entry_active.pop(target_zone, None)
            res = reserved_spots.get(spot)
            if res and res.get("plate") == plate:
                reserved_spots.pop(spot, None)
            upsert_car(plate, status="ENTRY_RETRY")
            log_decision(plate, "ENTRY_RECOVERY", f"Entry handling failed: {e}",
                         "Cleared gate ownership and queued a safe retry.")
            entry_queue.insert(0, item)
            threading.Timer(1.0, process_entry_queue).start()


def schedule_exit(plate, planned_minutes):
    seconds = max(1, int(planned_minutes)) * 60
    log_decision(plate, "TIMER", f"Exit requested in {seconds}s")
    timer = threading.Timer(seconds, request_exit_lane, args=(plate,))
    timer.daemon = True
    timer.start()


def request_exit_lane(plate):
    """Acquire Stage A: the single ExitSpot/payment-bay slot.

    Due cars remain in their parking bays while another car owns Stage A.
    Crucially, Stage A is released at ExitSpot CarOut; PARKMIND does NOT wait
    for final LeaveParking before allowing the next car to approach/pay.
    Stage B (escape corridor) is protected by a separate interlock.
    """
    global exit_lane_plate

    car = get_car(plate)
    if not car or car.get("status") not in ("PARKED", "WAITING_EXIT_LANE"):
        return

    # First acquire the ONE physical exit slot. If someone else owns it, this
    # car stays parked; we do NOT send it toward any ExitSpot.
    with state_lock:
        if exit_lane_plate is None:
            exit_lane_plate = plate
        elif exit_lane_plate != plate:
            if plate not in to_exit_queue:
                to_exit_queue.append(plate)
            upsert_car(plate, status="WAITING_EXIT_LANE")
            log_decision(
                plate, "EXIT_APPROACH_WAIT",
                f"Staying in parking bay; exit approach owned by {exit_lane_plate}.",
                "Single-car exit interlock prevents physical exit pile-up."
            )
            return

    zone_name = _component_zone(car.get("actual_spot") or car.get("assigned_spot") or "")

    # Source-zone crossing is still protected from opposing traffic. The car
    # remains in its bay while waiting; the global exit slot remains reserved
    # for it so another car cannot overtake into the physical exit area.
    if zone_name and zone_crossing_busy(zone_name, excluding_plate=plate):
        upsert_car(plate, status="WAITING_EXIT_LANE")
        timer = threading.Timer(0.50, request_exit_lane, args=(plate,))
        timer.daemon = True
        timer.start()
        return

    with state_lock:
        if zone_name:
            exit_route_zone[plate] = zone_name

    upsert_car(plate, status="TO_EXIT")
    append_journey(plate, "sent_to_exit")

    try:
        route_gates = open_route_gates(zone_name, reason=f"to-exit:{plate}", perimeter_role=None)
        log_decision(
            plate, "EXIT_ROUTE_GATES",
            f"zone={zone_name or 'UNASSIGNED'} gates={route_gates}",
            "Only this vehicle owns the physical exit approach."
        )
        send_car(plate, "exit")
    except Exception as e:
        with state_lock:
            exit_route_zone.pop(plate, None)
            if exit_lane_plate == plate:
                exit_lane_plate = None
            if plate in to_exit_queue:
                to_exit_queue.remove(plate)
        close_zone_if_idle(zone_name)
        upsert_car(plate, status="WAITING_EXIT_LANE")
        log_decision(plate, "EXIT_DISPATCH_ERROR", str(e),
                     "Released exit interlock so another parked car can try.")
        threading.Timer(0.2, dispatch_next_exit_lane).start()


def dispatch_next_exit_lane():
    """Release the next due car only after the prior car physically departs."""
    global exit_lane_plate
    next_plate = None
    with state_lock:
        if exit_lane_plate is not None:
            return
        while to_exit_queue:
            candidate = to_exit_queue.pop(0)
            car = get_car(candidate)
            if car and car.get("status") in ("PARKED", "WAITING_EXIT_LANE"):
                exit_lane_plate = candidate
                next_plate = candidate
                break

    if next_plate:
        threading.Thread(target=request_exit_lane, args=(next_plate,), daemon=True).start()


# =====================================================================
# [INNOVATION #3 + #9] — MULTI-FACTOR PAYMENT VERIFICATION
# =====================================================================
def calculate_charge(plate, exit_time_str):
    """Use the payment calculation from the working Level-2 controller.

    Simulator charge is based on whole/partial minutes from confirmed parking
    arrival until ExitSpot arrival. Any positive partial minute rounds up.
    Electric vehicles pay the same minute amount again as chargingCost.
    Timestamps retain fractional seconds so boundary rounding matches the
    simulator rather than silently truncating precision.
    """
    car = get_car(plate)
    if not car:
        return (1, 1.0, 0.0)

    parked_dt = _parse_sim_time(car.get("parked_time"))
    exit_dt = _parse_sim_time(exit_time_str)
    planned = max(0, int(car.get("planned_minutes") or 0))

    if parked_dt and exit_dt:
        stay_seconds = max(0.0, (exit_dt - parked_dt).total_seconds())
        minutes = max(1, math.ceil(stay_seconds / 60.0))
        # Never manufacture an extra minute merely because PARKMIND sent the
        # vehicle toward the exit.  The simulator's timestamps are the source
        # of truth: math.ceil(stay_seconds / 60) already charges a genuine
        # partial minute when observable travel pushes the stay across a minute
        # boundary.  We only clamp upward to the requested dwell duration if
        # timestamp jitter would otherwise bill less than the planned stay.
        scheduled_dwell = max(1, planned) if planned else 1
        if planned and minutes < scheduled_dwell:
            log_decision(
                plate, "BILLING_GUARD",
                f"elapsed={minutes}min scheduled_minimum={scheduled_dwell}min planned={planned}min",
                "Clamped to planned dwell only; no unconditional +1 travel minute."
            )
            minutes = scheduled_dwell
    else:
        minutes = max(1, planned or 1)
        log_decision(
            plate, "BILLING_TIME_FALLBACK",
            f"parked={car.get('parked_time')} exit={exit_time_str}",
            "Simulator timestamp could not be parsed; planned duration used as emergency fallback."
        )

    if planned and minutes > planned * ANOMALY_PARKING_MULTIPLIER:
        upsert_car(plate, anomaly_flag=1)
        log_anomaly(plate, "OVER_PARK",
                    f"Parked {minutes}min vs planned {planned}min (>10x)")

    parking_cost = float(minutes)
    is_electric = "electric" in str(car.get("car_type", "")).lower()
    charging_cost = float(minutes) if is_electric else 0.0
    return minutes, parking_cost, charging_cost


def verify_payment(plate, received_amount):
    """
    Multi-factor payment verification:
      [FACTOR 1] Car was tracked from entry (not a ghost).
      [FACTOR 2] Amount matches our calculation exactly.
      [FACTOR 3] No prior payment already accepted.
      [FACTOR 4] Car status indicates AT_EXIT / WAITING_TO_CHARGE.
    Returns (ok: bool, factors: str)
    """
    car = get_car(plate)
    if not car:
        log_fraud(plate, "GHOST_CAR — payment for untracked plate", received_amount)
        return False, "GHOST_CAR"

    factors = []

    # Factor 1: was tracked
    if not car.get("entry_time"):
        log_fraud(plate, "GHOST_CAR — no entry_time recorded", received_amount)
        return False, "GHOST_CAR_NO_ENTRY"
    factors.append("TRACKED=✓")

    # Factor 2: amount matches exactly
    expected = float(car.get("expected_amount") or 0)
    if abs(received_amount - expected) > 0.001:
        if received_amount < expected:
            reason = f"INSUFFICIENT_FUNDS expected={expected} got={received_amount}"
            log_fraud(plate, reason, received_amount)
            return False, f"INSUFFICIENT_FUNDS(expected={expected}, received={received_amount})"
        reason = f"AMOUNT_MISMATCH expected={expected} got={received_amount}"
        log_fraud(plate, reason, received_amount)
        return False, f"AMOUNT_MISMATCH(expected={expected})"
    factors.append(f"AMOUNT_MATCH=✓({expected})")

    # Factor 3: not already paid
    if car.get("payment_status") == "PAID":
        log_fraud(plate, "DOUBLE_PAYMENT — already paid", received_amount)
        return False, "DOUBLE_PAYMENT"
    factors.append("NOT_DOUBLE=✓")

    # Factor 4: status sanity
    if car.get("status") not in ("AT_EXIT", "PAYMENT_PENDING", "WAITING_TO_CHARGE", "PAYMENT_HOLD", "TO_EXIT"):
        log_fraud(plate,
                  f"BAD_STATUS — status={car.get('status')}",
                  received_amount)
        return False, f"BAD_STATUS({car.get('status')})"
    factors.append("STATUS_OK=✓")

    return True, " | ".join(factors)


def payment_timeout_watch(plate):
    """Never double-charge. Hold the one exit vehicle if payment is unresolved.

    A 201 response from /charge is only transport-level acceptance. PARKMIND
    never performs a speculative retry while that request is unresolved. A
    retry is permitted only after the simulator explicitly rejects the prior
    attempt as "not waiting at exit".
    """
    time.sleep(PAYMENT_TIMEOUT_SEC)
    car = get_car(plate)
    if not car or car.get("payment_status") == "PAID":
        return
    if car.get("status") not in ("AT_EXIT", "PAYMENT_PENDING", "PAYMENT_HOLD", "TO_EXIT"):
        return

    upsert_car(plate, status="PAYMENT_HOLD")
    log_anomaly(
        plate, "PAYMENT_TIMEOUT",
        f"Charge request has not resolved after {PAYMENT_TIMEOUT_SEC}s; vehicle remains isolated at the single exit slot."
    )
    log_decision(
        plate, "PAYMENT_HOLD",
        "No speculative charge retry sent; exit gate remains closed.",
        "Only an explicit simulator rejection can arm another charge attempt."
    )


def schedule_charge_attempt(plate, parking_cost, charging_cost, trigger="initial"):
    """Schedule exactly one charge attempt using bounded backoff.

    HTTP 201 from /charge is not treated as proof of payment.  PARKMIND sends
    the first attempt only after an exit-settle delay.  A later attempt is
    scheduled only when the simulator explicitly rejects the previous one with
    the known "Car should be charged at the exit" penalty.  This preserves the
    no-double-charge invariant while still recovering from the ExitSpot race.
    """
    with state_lock:
        if plate in charge_attempt_scheduled:
            return False
        sent = int(charge_attempt_counts.get(plate, 0) or 0)

    if sent >= len(CHARGE_RETRY_DELAYS):
        upsert_car(plate, status="PAYMENT_HOLD")
        log_decision(
            plate, "CHARGE_RETRY_EXHAUSTED",
            f"All {len(CHARGE_RETRY_DELAYS)} charge attempts were explicitly rejected.",
            "Vehicle remains isolated at ExitSpot; no speculative extra charge is sent."
        )
        return False

    delay = float(CHARGE_RETRY_DELAYS[sent])
    attempt_number = sent + 1

    with state_lock:
        charge_attempt_scheduled.add(plate)

    timer = threading.Timer(
        delay,
        perform_charge_attempt,
        args=(plate, float(parking_cost), float(charging_cost), attempt_number, trigger),
    )
    timer.daemon = True
    timer.start()

    log_decision(
        plate, "CHARGE_RETRY_ARMED" if sent else "CHARGE_ARMED",
        f"attempt={attempt_number}/{len(CHARGE_RETRY_DELAYS)} delay={delay:.1f}s",
        f"trigger={trigger}; waiting for simulator ExitSpot state to settle."
    )
    return True


def perform_charge_attempt(plate, parking_cost, charging_cost, attempt_number, trigger="timer"):
    """Send /charge only when this trip is explicitly ready for an attempt."""
    with state_lock:
        charge_attempt_scheduled.discard(plate)
        latest = get_car(plate) or {}

        # payment_made is authoritative.  A late timer must never charge again.
        if latest.get("payment_status") in ("PAID", "INVALID", "CHARGE_ERROR"):
            return

        # REQUESTED/PAYMENT_PENDING means an earlier HTTP 201 is unresolved.
        # Never retry merely because a timer elapsed; wait for payment_made or
        # the simulator's explicit not-waiting penalty to reset this state.
        if latest.get("payment_status") != "WAITING_TO_CHARGE":
            log_decision(
                plate, "CHARGE_ATTEMPT_SKIPPED",
                f"attempt={attempt_number} payment_status={latest.get('payment_status')}",
                "Previous charge request is unresolved; double-charge prevention wins."
            )
            return

        if latest.get("status") not in ("AT_EXIT", "PAYMENT_HOLD"):
            log_decision(
                plate, "CHARGE_ATTEMPT_SKIPPED",
                f"attempt={attempt_number} car_status={latest.get('status')}",
                "Vehicle is no longer in a chargeable exit state."
            )
            return

        charge_attempt_counts[plate] = max(
            int(charge_attempt_counts.get(plate, 0) or 0),
            int(attempt_number),
        )
        upsert_car(plate, payment_status="REQUESTED", status="PAYMENT_PENDING")

    try:
        charge_car(plate, parking_cost, charging_cost)
        log_decision(
            plate, "CHARGE_ATTEMPT",
            f"attempt={attempt_number}/{len(CHARGE_RETRY_DELAYS)} parking={parking_cost} charging={charging_cost}",
            f"trigger={trigger}; HTTP acceptance is not treated as payment success."
        )
    except Exception as exc:
        upsert_car(plate, payment_status="CHARGE_ERROR", status="PAYMENT_HOLD")
        log_decision(plate, "CHARGE_ERROR", str(exc))


def _exit_release_required_gates(plate):
    """Derive a safe egress path from only API-discovered topology.

    The barrier API gives name + zoneParent but does not identify which empty-zone
    perimeter barrier is the true exit. To avoid guessing the wrong one, PARKMIND
    opens the actual ExitSpot's zone barriers plus ALL API-returned perimeter
    barriers for the brief one-car departure window. They are closed immediately
    after ExitSpot CarOut.
    """
    exit_spot = exit_spot_by_plate.get(plate, "")
    exit_zone = _component_zone(exit_spot) if exit_spot else ""
    required = []
    for name in list(zone_gates.get(exit_zone or "", [])) + list(perimeter_gates):
        if name and name not in required:
            required.append(name)
    return exit_zone, required


def payment_release_authorized(plate, require_exit_owner=True):
    """Hard payment interlock for EVERY paid-exit release path.

    A vehicle is allowed to receive goto/leavepark only when:
      1) it is physically at the ExitSpot,
      2) a payment_made webhook was accepted and stored as PAID,
      3) the received amount exactly matches PARKMIND's expected charge, and
      4) it owns the physical ExitSpot slot (when requested).

    This intentionally duplicates the final payment check at the lowest release
    layer. Even if a higher-level callback, gate webhook, retry, or operator path
    calls an exit-release helper by mistake, an unpaid car still cannot leave.
    """
    car = get_car(plate) or {}
    status = str(car.get("payment_status") or "")
    expected = float(car.get("expected_amount") or car.get("total_charge") or 0.0)
    actual = float(car.get("actual_paid") or 0.0)

    if not car.get("exit_arrival_time"):
        return False, "NOT_AT_EXIT"
    if status != "PAID":
        return False, f"PAYMENT_NOT_CONFIRMED({status or 'NONE'})"
    if abs(actual - expected) > 0.001:
        return False, f"PAID_AMOUNT_MISMATCH(expected={expected}, actual={actual})"

    if require_exit_owner:
        with state_lock:
            owns_lane = (exit_lane_plate == plate)
            owns_physical_queue = bool(physical_exit_queue and physical_exit_queue[0] == plate)
        if not owns_lane or not owns_physical_queue:
            return False, "NOT_PHYSICAL_EXIT_OWNER"

    return True, f"PAID_OK(expected={expected}, actual={actual})"


def try_release_paid_when_ready(plate):
    """Send leavepark once, after VERIFIED payment and every required gate is Open."""
    global exit_active

    authorized, why = payment_release_authorized(plate, require_exit_owner=True)
    if not authorized:
        log_decision(
            plate, "EXIT_RELEASE_PAYMENT_BLOCK", why,
            "NO PAYMENT = NO LEAVE. goto/leavepark was NOT sent."
        )
        return False

    with state_lock:
        active = dict(exit_active or {})
        if (
            not active
            or active.get("plate") != plate
            or active.get("sent")
            or active.get("release_inflight")
        ):
            return False
        required = list(active.get("required_gates") or [])

    if required and not all(gate_fully_open(name) for name in required):
        return False

    # Multiple gate_action webhooks can concurrently discover that the final
    # required barrier is open. Claim the release atomically before POSTing so
    # only one thread can issue goto/leavepark.
    with state_lock:
        if (
            not exit_active
            or exit_active.get("plate") != plate
            or exit_active.get("sent")
            or exit_active.get("release_inflight")
        ):
            return False
        if required and not all(gate_fully_open(name) for name in required):
            return False
        exit_active["release_inflight"] = True

    try:
        send_car(plate, "leavepark")
        with state_lock:
            if exit_active and exit_active.get("plate") == plate:
                exit_active["sent"] = True
                exit_active["release_inflight"] = False
        log_decision(
            plate, "EXIT_RELEASE",
            f"All egress barriers OPEN; leavepark sent once. gates={required}",
            "Atomic release claim prevents duplicate leavepark commands from concurrent gate webhooks."
        )
        return True
    except Exception as e:
        with state_lock:
            if exit_active and exit_active.get("plate") == plate:
                exit_active["release_inflight"] = False
        log_decision(plate, "EXIT_RELEASE_ERROR", str(e), f"required_gates={required}")
        return False


def ensure_gate_open_and_leave(plate):
    """Acquire Stage B only for a VERIFIED-PAID car, then prepare egress.

    No payment_made confirmation means the vehicle remains at ExitSpot and no
    egress gate/open or goto/leavepark command is allowed for that vehicle.
    """
    global exit_active, escape_corridor_plate

    authorized, why = payment_release_authorized(plate, require_exit_owner=True)
    if not authorized:
        car = get_car(plate) or {}
        if car.get("exit_arrival_time") and car.get("payment_status") != "PAID":
            upsert_car(plate, status="PAYMENT_HOLD")
        log_decision(
            plate, "EXIT_LOCKED_UNPAID", why,
            "Payment interlock: vehicle remains at ExitSpot; no leavepark command sent."
        )
        return False

    exit_zone, required = _exit_release_required_gates(plate)

    with state_lock:
        if escape_corridor_plate not in (None, plate):
            upsert_car(plate, status="PAID_WAIT_ESCAPE")
            log_decision(
                plate, "ESCAPE_CORRIDOR_WAIT",
                f"Paid and holding at ExitSpot; escape corridor owned by {escape_corridor_plate}.",
                "Two-stage pipeline: payment may complete early, but only one car enters escape corridor."
            )
            return False

        if exit_active and exit_active.get("plate") != plate:
            upsert_car(plate, status="PAID_WAIT_ESCAPE")
            log_decision(
                plate, "EXIT_RELEASE_BLOCKED",
                f"Physical release still owned by {exit_active.get('plate')}",
                "Stage-B interlock."
            )
            return False

        # If this same paid car already owns Stage B, another concurrent
        # payment/gate callback must not recreate the release state or reopen
        # gates. It may only re-check whether release is now ready.
        if (
            escape_corridor_plate == plate
            and exit_active
            and exit_active.get("plate") == plate
        ):
            already_owned = True
        else:
            already_owned = False
            escape_corridor_plate = plate
            exit_active = {
                "plate": plate,
                "sent": False,
                "release_inflight": False,
                "required_gates": list(required),
                "exit_zone": exit_zone,
            }

    if already_owned:
        return try_release_paid_when_ready(plate)

    for name in required:
        try:
            if str(gates.get(name, {}).get("state") or "") != "Open":
                open_gate(name)
        except Exception as e:
            log_decision(plate, "EXIT_GATE_OPEN_ERROR", f"{name}: {e}")

    if not try_release_paid_when_ready(plate):
        log_decision(
            plate, "EXIT_INTERLOCK_WAIT",
            f"Waiting for egress gates to become fully Open: {required}",
            f"actual ExitSpot={exit_spot_by_plate.get(plate) or 'unknown'} zone={exit_zone or 'unassigned'}"
        )
    return True

def close_exit_if_idle():
    with state_lock:
        if (physical_exit_queue or exit_active is not None or
                exit_lane_plate is not None or escape_corridor_plate is not None):
            return
        # Never include the spawn-critical entrance barrier in exit cleanup.
        names = [name for name in perimeter_gates if name != ENTRY_GATE]
    for name in names:
        try:
            close_gate(name)
        except Exception:
            pass
    ensure_keep_open_gates()


# =====================================================================
# ENTRY TRANSIT RECOVERY WATCHDOG
# =====================================================================
def entry_transit_watchdog():
    """
    Supervise every car AFTER it has cleared the entry gate.

    Multiple vehicles may now be in transit simultaneously because each bay is
    already reserved. A slow vehicle no longer blocks the next arrival.
    """
    global entry_active

    while True:
        time.sleep(2)

        with state_lock:
            snapshot = {
                plate: dict(info)
                for plate, info in in_transit.items()
            }

        for plate, active in snapshot.items():
            sent_at = active.get("sent_at")
            if not sent_at:
                continue

            elapsed = time.time() - float(sent_at)
            spot = active["spot"]
            retries = int(active.get("route_retries") or 0)

            car = get_car(plate)
            if car and car.get("status") == "PARKED":
                with state_lock:
                    in_transit.pop(plate, None)
                continue

            with state_lock:
                is_at_entry = (entry_active and entry_active.get("plate") == plate)

            if is_at_entry:
                if elapsed < ENTRY_TRANSIT_TIMEOUT_SEC:
                    continue

                if retries < ENTRY_TRANSIT_MAX_RETRIES:
                    try:
                        send_car(plate, spot)
                        with state_lock:
                            if plate in in_transit:
                                in_transit[plate]["route_retries"] = retries + 1
                                in_transit[plate]["sent_at"] = time.time()
                        log_decision(plate, "ENTRY_TRANSIT_RETRY", f"Re-sent route to {spot} ({retries + 1}/{ENTRY_TRANSIT_MAX_RETRIES})")
                        with state_lock:
                            stats["auto_recoveries"] += 1
                        continue
                    except Exception as e:
                        log_decision(plate, "ENTRY_TRANSIT_RETRY_ERROR", str(e))
                        with state_lock:
                            if plate in in_transit:
                                in_transit[plate]["sent_at"] = time.time()
                        continue
                else:
                    try:
                        send_car(plate, "leavepark")
                        log_decision(plate, "ENTRY_TRANSIT_ABORT", f"Vehicle stuck at entrance; redirected out.")
                    except Exception as e:
                        log_decision(plate, "ENTRY_TRANSIT_ABORT_ERROR", str(e))
                    
                    with state_lock:
                        res = reserved_spots.get(spot)
                        if res and res.get("plate") == plate:
                            reserved_spots.pop(spot, None)
                        in_transit.pop(plate, None)
                        if entry_active and entry_active.get("plate") == plate:
                            entry_active = None

                    release_zone_entry_lock(plate)
                    upsert_car(plate, status="ENTRY_ABORTED", decision=f"Stuck at entrance.")
                    threading.Timer(0.1, process_entry_queue).start()
            else:
                if elapsed > 90:
                    log_decision(plate, "TRANSIT_LOST", f"Car did not arrive at {spot} after 90s.")
                    with state_lock:
                        res = reserved_spots.get(spot)
                        if res and res.get("plate") == plate:
                            reserved_spots.pop(spot, None)
                        in_transit.pop(plate, None)
                    release_zone_entry_lock(plate)
                    upsert_car(plate, status="LOST", decision="Lost in transit")
                    threading.Timer(0.1, process_entry_queue).start()



# =====================================================================
# [INNOVATION #4] — SPOT RESERVATION TTL AUTO-RELEASE
# =====================================================================
def reservation_ttl_watchdog():
    """Background thread that releases stale reservations."""
    while True:
        time.sleep(5)
        now = time.time()
        with state_lock:
            stale = [n for n, r in reserved_spots.items()
                     if now - r.get("reserved_at", now) > RESERVATION_TTL_SEC]
            for n in stale:
                plate = reserved_spots[n]["plate"]

                # Never free the spot while that exact car is still the active
                # entry movement. That would allow another car to "fight" for it.
                active_at_gate = (
                    entry_active
                    and entry_active.get("plate") == plate
                    and entry_active.get("spot") == n
                )
                active_in_transit = (
                    plate in in_transit
                    and in_transit[plate].get("spot") == n
                )

                if active_at_gate or active_in_transit:
                    reserved_spots[n]["reserved_at"] = now
                    log_decision(
                        plate, "RESERVATION_EXTENDED",
                        f"{n} kept reserved because the assigned car is still active/in transit.",
                        "Collision shield: reservations remain locked until physical parking confirmation."
                    )
                    continue

                log_decision(plate, "RESERVATION_EXPIRED",
                             f"Spot {n} auto-released after TTL",
                             f"TTL={RESERVATION_TTL_SEC}s — inactive reservation")
                reserved_spots.pop(n, None)
                car = get_car(plate)
                if car and car.get("status") == "ASSIGNED":
                    upsert_car(plate, status="LOST",
                               decision="Inactive reservation expired.")


# =====================================================================
# [INNOVATION #11] — STUCK-STATE RECOVERY WATCHDOG
# =====================================================================
def stuck_state_watchdog():
    """Detect cars stuck in transient states and retry their operations."""
    while True:
        time.sleep(15)
        try:
            conn = db()
            rows = conn.execute(
                "SELECT plate, status, assigned_spot, exit_arrival_time FROM cars "
                "WHERE status IN ('TO_EXIT', 'PAYMENT_PENDING', 'WAITING_TO_CHARGE', 'ASSIGNED')"
            ).fetchall()
            conn.close()
            now = datetime.now()
            for r in rows:
                plate = r["plate"]
                status = r["status"]
                # Check assigned cars — if reserved too long, the TTL watchdog handles it.
                if status == "TO_EXIT":
                    # Was the car sent to exit a while ago but never arrived?
                    # We can't easily know timestamps here; just nudge.
                    car = get_car(plate)
                    if not car:
                        continue
                    # Resend exit command (idempotent if already there).
                    try:
                        send_car(plate, "exit")
                        log_decision(plate, "STUCK_RECOVERY",
                                     "Re-sent exit command (was TO_EXIT too long)",
                                     "watchdog retry")
                        with state_lock:
                            stats["auto_recoveries"] += 1
                    except Exception:
                        pass
        except Exception as e:
            print("[WATCHDOG ERROR]", e)


def finalize_leaveparking_departure(plate, spot_name, server_time, event_key="", source="LeaveParking CarOut"):
    """Finalize Stage B only when the car is clear of the escape corridor.

    LeaveParking CarOut is authoritative.  Some simulator builds may emit only
    CarIn, so CarIn arms a short fallback timer instead of releasing Stage B
    immediately.  The in-flight guard keeps CarOut and that fallback idempotent.
    """
    global exit_active, escape_corridor_plate, api_escape_unknown_count

    with state_lock:
        if plate in leave_finalize_inflight:
            return False
        leave_finalize_inflight.add(plate)

    try:
        car_before_departure = get_car(plate) or {}
        if (
            car_before_departure.get("departure_time")
            and car_before_departure.get("status") in ("LEFT", "ESCAPED_UNPAID")
        ):
            return False

        # For the CarIn fallback, advance the webhook timestamp using the latest
        # simulator-anchored clock so stay duration is not frozen at node entry.
        if source.endswith("fallback"):
            estimated_now = simulator_now()
            if estimated_now is not None:
                server_time = estimated_now.isoformat(sep=" ")

        total_stay_seconds = seconds_between(
            car_before_departure.get("entry_time"),
            server_time,
        )
        if total_stay_seconds > 0:
            total_stay_minutes = max(1, math.ceil(total_stay_seconds / 60))
        else:
            total_stay_minutes = int(car_before_departure.get("billable_minutes") or 0)

        paid = car_before_departure.get("payment_status") == "PAID"
        final_status = "LEFT" if paid else "ESCAPED_UNPAID"

        if not paid:
            record_alert(
                alert_key=f"UNPAID_LEAVEPARK:{plate}:{event_key or server_time}",
                alert_type="ESCAPED WITHOUT PAYMENT",
                severity="CRITICAL",
                plate=plate,
                reason=(
                    "Vehicle cleared LeaveParking with payment status "
                    f"{car_before_departure.get('payment_status') or 'NONE'}."
                ),
                event_time=server_time,
            )

        upsert_car(
            plate,
            departure_time=server_time,
            total_stay_minutes=total_stay_minutes,
            total_stay_seconds=total_stay_seconds,
            status=final_status,
        )
        resolve_exit_stuck_alert(plate)
        append_journey(plate, f"left_park@{spot_name}")
        log_decision(
            plate, "PARK_DEPARTURE_CONFIRMED", spot_name,
            f"{source} confirmed/finalized escape-corridor clearance."
        )

        with state_lock:
            stats["total_departed"] += 1
            charge_attempt_counts.pop(plate, None)
            charge_attempt_scheduled.discard(plate)

        car_after = get_car(plate) or {}
        with state_lock:
            stats["total_revenue"] += float(car_after.get("actual_paid") or 0)

        assigned = car_after.get("assigned_spot")
        if assigned:
            try:
                parked_dt = _parse_sim_time(car_after.get("parked_time"))
                leave_dt = _parse_sim_time(server_time)
                dur_min = (
                    max(1, math.ceil((leave_dt - parked_dt).total_seconds() / 60.0))
                    if parked_dt and leave_dt
                    else int(car_after.get("planned_minutes") or 1)
                )
            except Exception:
                dur_min = int(car_after.get("planned_minutes") or 1)
            update_spot_stats(assigned, dur_min, float(car_after.get("actual_paid") or 0))

        departing_route_gates = []
        next_paid_plate = None
        should_dispatch_stage_a = False

        with state_lock:
            if exit_active and exit_active.get("plate") == plate:
                departing_route_gates = list(exit_active.get("required_gates") or [])
                exit_active = None

            if escape_corridor_plate == plate:
                escape_corridor_plate = None
            elif escape_corridor_plate == "__API_ESCAPE_OCCUPIED__":
                api_escape_unknown_count = max(0, api_escape_unknown_count - 1)
                if api_escape_unknown_count == 0:
                    escape_corridor_plate = None

            if plate in physical_exit_queue:
                physical_exit_queue.remove(plate)
            if plate in to_exit_queue:
                to_exit_queue.remove(plate)
            exit_route_zone.pop(plate, None)
            exit_spot_by_plate.pop(plate, None)

            # Stage A may already contain the next car.  If it is paid, hand
            # Stage B to it only after this finalizer has actually cleared the
            # previous escape-corridor owner.
            if exit_lane_plate and not str(exit_lane_plate).startswith("__API_"):
                candidate = get_car(exit_lane_plate) or {}
                if candidate.get("payment_status") == "PAID":
                    next_paid_plate = exit_lane_plate
            elif exit_lane_plate is None and api_exit_unknown_count == 0:
                should_dispatch_stage_a = True

        next_required = set()
        if next_paid_plate:
            try:
                _next_zone, _next_required = _exit_release_required_gates(next_paid_plate)
                next_required = set(_next_required)
            except Exception:
                next_required = set()

        for gate_name in departing_route_gates:
            if gate_name in next_required:
                continue
            try:
                if str(gates.get(gate_name, {}).get("state") or "") in ("Open", "Opening"):
                    close_gate(gate_name)
            except Exception:
                pass

        if next_paid_plate:
            threading.Thread(
                target=ensure_gate_open_and_leave,
                args=(next_paid_plate,),
                daemon=True,
            ).start()
        elif should_dispatch_stage_a:
            timer = threading.Timer(EXIT_STAGE_A_CLEARANCE_SEC, dispatch_next_exit_lane)
            timer.daemon = True
            timer.start()

        return True
    finally:
        with state_lock:
            leave_finalize_inflight.discard(plate)


# =====================================================================
# WEBHOOK PROCESSING
# =====================================================================
def handle_event(data):
    global entry_active, exit_active, exit_lane_plate, escape_corridor_plate, physical_exit_queue, api_exit_unknown_count, api_escape_unknown_count, last_simulator_time, last_simulator_time_seen_at
    event_class = data.get("EventClass")
    server_clock = data.get("ServerDateTime")
    if server_clock:
        last_simulator_time = str(server_clock)
        last_simulator_time_seen_at = time.monotonic()

    try:
        if event_class == "gate_action":
            name = data.get("Name")
            action = data.get("Action")
            with state_lock:
                if name not in gates:
                    gates[name] = {"name": name, "usage_count": 0}
                prev_state = gates[name].get("state")
                gates[name]["state"] = action
                # Count open/close cycles for wear tracking.
                if prev_state in ("Open", "Closed") and action in ("Opening", "Closing"):
                    gates[name]["usage_count"] = gates[name].get("usage_count", 0) + 1

                # Any required barrier may complete the JIT interlock.
                # Release occurs only when ALL required barriers are fully Open.
                if action == "Open" and entry_active and not entry_active.get("sent"):
                    required = list(entry_active.get("required_gates") or [])
                    if name in required:
                        plate = entry_active.get("plate")
                        threading.Thread(target=try_dispatch_entry_when_ready,
                                         args=(plate,), daemon=True).start()

                if name == ENTRY_GATE and action == "Closed":
                    if entry_active is None and entry_queue:
                        threading.Timer(0.05, process_entry_queue).start()

                # Paid exit release uses the same strict interlock as entry:
                # every API-derived egress barrier must report state == Open.
                if action == "Open" and exit_active and not exit_active.get("sent"):
                    required = list(exit_active.get("required_gates") or [])
                    if name in required:
                        plate = exit_active.get("plate")
                        threading.Thread(target=try_release_paid_when_ready,
                                         args=(plate,), daemon=True).start()

        elif event_class == "car_spot_action":
            plate = data.get("CarPlateNumber")
            car_type = data.get("CarType") or "Normal"
            spot_name = data.get("SpotName")
            spot_type = data.get("SpotType")
            direction = data.get("Direction")
            server_time = data.get("ServerDateTime") or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            planned = int(data.get("PlannedParkingDurationInMinutes") or 0)

            if spot_type == "EntrySpot" and direction == "CarIn":
                existing = get_car(plate) or {}
                active_trip = bool(existing.get("entry_time")) and existing.get("status") not in ("LEFT", "ESCAPED_UNPAID", "LEFT_FULL", "ENTRY_ABORTED", "LOST")
                if not active_trip:
                    with state_lock:
                        stats["total_arrivals"] += 1
                    upsert_car(
                        plate, car_type=car_type, planned_minutes=planned,
                        entry_time=server_time, parked_time=None, exit_arrival_time=None,
                        departure_time=None, expected_amount=0, actual_paid=0,
                        billable_minutes=0, parking_cost=0, charging_cost=0,
                        total_charge=0, payment_status="NONE", status="WAITING"
                    )
                    append_journey(plate, "arrived")
                else:
                    # Do not overwrite the trip's original timing if the simulator
                    # reports another EntrySpot CarIn for the same active vehicle.
                    upsert_car(plate, car_type=car_type, planned_minutes=planned or existing.get("planned_minutes"), status=existing.get("status") or "WAITING")
                    log_decision(plate, "DUPLICATE_ENTRY_SENSOR", f"Ignored timing reset from {spot_name}", "Original trip entry/parking timestamps preserved for billing.")
                log_decision(plate, "ARRIVAL",
                             f"Arrived at {spot_name}; planned {planned} min; type {car_type}")

                if not active_trip:
                    with state_lock:
                        already_queued = any(x["plate"] == plate for x in entry_queue)
                        already_active = entry_active and entry_active["plate"] == plate
                        if not already_queued and not already_active:
                            entry_queue.append({"plate": plate, "car_type": car_type, "planned": planned})
                    process_entry_queue()

            elif spot_type == "EntrySpot" and direction == "CarOut":
                # THIS is the correct point to release the entrance pipeline.
                # We do NOT wait until the previous vehicle reaches its parking bay.
                append_journey(plate, "cleared_entry")
                log_decision(
                    plate,
                    "ENTRY_CLEARED",
                    f"Vehicle cleared {spot_name}.",
                    "Entry lane released immediately; next arrival may enter while this car drives to its reserved bay."
                )

                cleared_zone = ""
                with state_lock:
                    if entry_active and entry_active.get("plate") == plate:
                        cleared_zone = str(entry_active.get("zone") or "")
                        entry_active = None
                    waiting = bool(entry_queue)

                # The car has physically cleared the EntrySpot, so it is now
                # travelling through its already-open zone path. Keep that zone
                # marked busy for only a short clearance window — NOT until it parks.
                if cleared_zone:
                    ztimer = threading.Timer(
                        ZONE_GATE_CLEARANCE_SEC,
                        complete_zone_entry_crossing,
                        args=(plate, cleared_zone),
                    )
                    ztimer.daemon = True
                    ztimer.start()

                # Close the API-derived entry perimeter barrier after every car.
                # This prevents the facility from looking permanently open. The
                # next queued vehicle will reopen it when its zone path is free.
                if ENTRY_GATE:
                    # Do not slam the perimeter gate shut 50 ms after CarOut.
                    # More importantly, re-check the live entry pipeline when
                    # the timer fires so a back-to-back arrival cannot have the
                    # gate closed underneath it.
                    timer = threading.Timer(ENTRY_GATE_IDLE_CLOSE_SEC, close_entry_if_idle)
                    timer.daemon = True
                    timer.start()
                if waiting:
                    # process_entry_queue will keep the next car at EntrySpot
                    # while the current car still owns the destination-zone gate.
                    threading.Timer(0.20, process_entry_queue).start()

            elif spot_type == "Park":
                with state_lock:
                    if spot_name in spots:
                        spots[spot_name]["occupied"] = (direction == "CarIn")
                        if direction == "CarIn":
                            spots[spot_name]["usage_count"] = spots[spot_name].get("usage_count", 0) + 1

                if direction == "CarIn":
                    car = get_car(plate) or {}
                    expected_spot = car.get("assigned_spot")

                    # Never silently overwrite the intended destination.
                    if expected_spot and expected_spot != spot_name:
                        handle_wrong_spot(plate, spot_name, server_time, planned)
                    else:
                        finish_successful_parking(plate, spot_name, server_time, planned)

                elif direction == "CarOut":
                    car = get_car(plate) or {}
                    if car.get("actual_spot") == spot_name:
                        upsert_car(plate, actual_spot=None)
                    log_decision(plate, "LEFT_SPOT", spot_name)
                    append_journey(plate, f"left_{spot_name}")

            elif spot_type == "ExitSpot" and direction == "CarIn":
                # The simulator tells us which physical ExitSpot was actually
                # chosen by goto/exit. Keep it so paid egress opens that spot's
                # real zone barriers rather than guessing from the parking bay.
                with state_lock:
                    exit_spot_by_plate[plate] = spot_name
                    if exit_lane_plate is None:
                        # Defensive recovery after restart/webhook gap.
                        exit_lane_plate = plate
                    elif exit_lane_plate != plate:
                        log_anomaly(plate, "EXIT_INTERLOCK_BREACH",
                                    f"Unexpected second car reached ExitSpot {spot_name} while {exit_lane_plate} owns exit lane")
                car_before_exit = get_car(plate) or {}
                manual_spot = car_before_exit.get("actual_spot") or car_before_exit.get("assigned_spot") or ""
                if str(car_before_exit.get("operator_note") or "").startswith("[MANUAL RECOVERY]") and manual_spot:
                    with state_lock:
                        if manual_spot in spots:
                            spots[manual_spot]["occupied"] = False
                        reserved_spots.pop(manual_spot, None)
                    log_decision(plate, "MANUAL_RECOVERY_SPOT_RELEASED", manual_spot,
                                 "ExitSpot arrival confirms the manually recovered vehicle has physically left its bay.")
                source_zone = _component_zone(
                    car_before_exit.get("actual_spot") or car_before_exit.get("assigned_spot") or ""
                )
                with state_lock:
                    exit_route_zone.pop(plate, None)
                close_zone_if_idle(source_zone)

                with state_lock:
                    if plate not in physical_exit_queue:
                        physical_exit_queue.append(plate)
                    is_front = (physical_exit_queue[0] == plate)
                
                car = get_car(plate)
                
                if car and car.get("payment_status") == "PAID":
                    if is_front:
                        ensure_gate_open_and_leave(plate)
                    return
                elif car and car.get("payment_status") in ("REQUESTED", "PAYMENT_HOLD", "PAYMENT_PENDING", "WAITING_TO_CHARGE"):
                    return
                    
                minutes, parking_cost, charging_cost = calculate_charge(plate, server_time)
                expected = parking_cost + charging_cost
                upsert_car(
                    plate,
                    exit_arrival_time=server_time,
                    billable_minutes=minutes,
                    parking_cost=parking_cost,
                    charging_cost=charging_cost,
                    total_charge=expected,
                    expected_amount=expected,
                    payment_status="WAITING_TO_CHARGE",
                    status="AT_EXIT"
                )
                append_journey(plate, "at_exit")
                log_decision(plate, "AT_EXIT",
                            f"{minutes} min; expected total={expected}",
                            f"parking={parking_cost}, charging={charging_cost}, "
                            f"electric={'yes' if charging_cost>0 else 'no'}")

                # New trip at ExitSpot: start a fresh bounded charge-attempt
                # sequence.  The first /charge is intentionally delayed so the
                # simulator can settle the car into its internal waiting state.
                with state_lock:
                    charge_attempt_counts[plate] = 0

                schedule_charge_attempt(
                    plate, parking_cost, charging_cost, trigger="ExitSpot CarIn"
                )
                threading.Thread(target=payment_timeout_watch, args=(plate,), daemon=True).start()

            elif spot_type == "ExitSpot" and direction == "CarOut":
                # IMPORTANT: ExitSpot CarOut is NOT final park departure.
                # The car has only left the payment bay and is still travelling
                # through the escape corridor toward a LeaveParking node.
                # Keep the global exit interlock and egress gates owned by this
                # car until a LeaveParking webhook confirms the corridor is clear.
                car_before_escape = get_car(plate) or {}
                paid = car_before_escape.get("payment_status") == "PAID"

                if not paid:
                    record_alert(
                        alert_key=f"UNPAID_EXIT_CORRIDOR:{plate}:{data.get('EventId') or data.get('SequenceId') or server_time}",
                        alert_type="LEFT EXIT BAY WITHOUT PAYMENT",
                        severity="CRITICAL",
                        plate=plate,
                        reason=(
                            f"Vehicle left ExitSpot toward escape corridor with payment status "
                            f"{car_before_escape.get('payment_status') or 'NONE'}."
                        ),
                        event_time=server_time
                    )

                upsert_car(
                    plate,
                    status="LEAVING_PARK" if paid else "LEAVING_UNPAID"
                )
                append_journey(plate, "left_exit_bay")
                log_decision(
                    plate, "EXIT_BAY_CLEARED", spot_name,
                    "Stage A is free immediately; Stage B remains owned by this car until LeaveParking."
                )

                release_stage_a = False
                with state_lock:
                    if plate in physical_exit_queue:
                        physical_exit_queue.remove(plate)
                    # PRACTICAL PIPELINE: free only the payment-bay slot here.
                    # The same car still owns escape_corridor_plate/exit_active.
                    if exit_lane_plate == plate:
                        exit_lane_plate = None
                        release_stage_a = True
                    elif exit_lane_plate == "__API_EXIT_OCCUPIED__":
                        api_exit_unknown_count = max(0, api_exit_unknown_count - 1)
                        if api_exit_unknown_count == 0:
                            exit_lane_plate = None
                            release_stage_a = True

                if release_stage_a:
                    if paid:
                        # CarOut is much stronger than CarIn, but give the
                        # simulator a brief node-clearance window before routing
                        # the next vehicle into the payment bay.
                        timer = threading.Timer(
                            EXIT_STAGE_A_CLEARANCE_SEC,
                            dispatch_next_exit_lane,
                        )
                        timer.daemon = True
                        timer.start()
                    else:
                        # This should never happen from PARKMIND because every
                        # leavepark path is payment-gated. If the simulator moves
                        # an unpaid vehicle anyway, do not feed another car into
                        # the same potentially unsafe exit condition.
                        log_decision(
                            plate, "EXIT_PIPELINE_FROZEN_UNPAID",
                            "Unpaid vehicle physically left ExitSpot; next exit dispatch withheld.",
                            "Failsafe after external/simulator movement outside PARKMIND's payment interlock."
                        )

            elif spot_type == "LeaveParking" and direction in ("CarIn", "CarOut"):
                event_key = str(
                    data.get("EventId")
                    or data.get("SequenceId")
                    or server_time
                )

                if direction == "CarIn":
                    # CarIn means the vehicle has reached the terminal node, not
                    # necessarily that the simulator has cleared the escape
                    # corridor yet.  Keep Stage B locked and arm a short fallback
                    # for simulator builds that never emit LeaveParking CarOut.
                    current = get_car(plate) or {}
                    if current.get("departure_time") and current.get("status") in ("LEFT", "ESCAPED_UNPAID"):
                        return

                    paid = current.get("payment_status") == "PAID"
                    upsert_car(
                        plate,
                        status="AT_LEAVE" if paid else "AT_LEAVE_UNPAID",
                    )
                    append_journey(plate, f"entered_leavepark@{spot_name}")
                    log_decision(
                        plate, "LEAVE_NODE_ENTERED", spot_name,
                        f"Stage B remains locked for {LEAVE_PARKING_CLEARANCE_SEC:.1f}s unless CarOut confirms clearance first."
                    )

                    timer = threading.Timer(
                        LEAVE_PARKING_CLEARANCE_SEC,
                        finalize_leaveparking_departure,
                        args=(
                            plate,
                            spot_name,
                            server_time,
                            event_key,
                            "LeaveParking CarIn fallback",
                        ),
                    )
                    timer.daemon = True
                    timer.start()
                    return

                # CarOut is the authoritative indication that the terminal node
                # and escape corridor have actually been cleared.
                finalize_leaveparking_departure(
                    plate,
                    spot_name,
                    server_time,
                    event_key,
                    "LeaveParking CarOut",
                )

        elif event_class == "payment_made":
            plate = data.get("CarPlateNumber")
            received = float(data.get("Amount") or 0)

            ok, factors = verify_payment(plate, received)
            upsert_car(plate, payment_factors=factors)

            if ok:
                with state_lock:
                    charge_attempt_counts.pop(plate, None)
                    charge_attempt_scheduled.discard(plate)
                # ONLY this verified payment webhook is allowed to mark the car PAID.
                # Exit-release helpers independently re-check this field + amount.
                upsert_car(plate, payment_status="PAID", status="PAID",
                           actual_paid=received)
                log_decision(plate, "PAYMENT_OK",
                             f"Expected matches received={received}",
                             f"FACTORS: {factors}. EXIT UNLOCKED FOR THIS CAR ONLY.")
                with state_lock:
                    is_physical_owner = (
                        exit_lane_plate == plate and
                        physical_exit_queue and physical_exit_queue[0] == plate
                    )
                if is_physical_owner:
                    ensure_gate_open_and_leave(plate)
                else:
                    log_decision(plate, "PAID_BUT_NOT_EXIT_OWNER",
                                 "Payment accepted but vehicle is not the active physical exit owner.",
                                 "Safety interlock prevents releasing the wrong car.")
            else:
                with state_lock:
                    charge_attempt_counts.pop(plate, None)
                    charge_attempt_scheduled.discard(plate)
                upsert_car(
                    plate,
                    payment_status="INVALID",
                    status="PAYMENT_HOLD",
                    actual_paid=received
                )
                log_decision(
                    plate, "PAYMENT_REJECTED",
                    f"received={received}",
                    f"FACTORS: {factors}. Gate remains CLOSED. "
                    "NO PAYMENT = NO LEAVE. A legitimate late/retry payment is allowed."
                )
                record_alert(
                    alert_key=f"PAYMENT_FAIL:{plate}:{data.get('EventId') or data.get('SequenceId') or datetime.now().timestamp()}",
                    alert_type="PAYMENT FAILURE",
                    severity="HIGH",
                    plate=plate,
                    reason=f"Payment rejected: {factors}",
                    event_time=data.get("ServerDateTime") or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                )

        elif event_class == "component_broken":
            name = str(data.get("Name") or "")
            with state_lock:
                for source in (spots, gates, fans, lights):
                    if name in source:
                        source[name]["broken"] = True
                if name:
                    alarms[name] = {
                        "name": name,
                        "problem": data.get("Problem") or "Require Maintenance"
                    }
            log_decision("", "COMPONENT_BROKEN", name,
                         "Failure came from simulator webhook; component isolated from automatic use.")

        elif event_class == "component_fixed":
            name = str(data.get("Name") or "")
            with state_lock:
                for source in (spots, gates, fans, lights):
                    if name in source:
                        source[name]["broken"] = False
                        source[name]["isUnderMaintenance"] = False
                alarms.pop(name, None)
                maintenance_requested.discard(name)
            log_decision("", "COMPONENT_FIXED", name,
                         "Simulator webhook cleared the cached failure/maintenance state.")

        elif event_class == "penalty":
            reason = data.get("Reason")
            fine = data.get("FineAmount")
            penalty_time = data.get("ServerDateTime") or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            penalty_plate = infer_plate_for_alert(
                data=data,
                reason=reason,
                event_time=penalty_time
            )

            lower_reason = str(reason or "").lower()

            # Targeted recovery for the simulator race where ExitSpot CarIn has
            # fired but the internal car state is not yet "waiting at exit".
            # This penalty is authoritative evidence that the previous /charge
            # did NOT apply, so and only so is another bounded attempt allowed.
            if penalty_plate and "should be charged at the exit" in lower_reason:
                car = get_car(penalty_plate) or {}
                if car.get("payment_status") in ("REQUESTED", "PAYMENT_PENDING"):
                    upsert_car(
                        penalty_plate,
                        payment_status="WAITING_TO_CHARGE",
                        status="AT_EXIT",
                    )
                    log_decision(
                        penalty_plate, "CHARGE_RESET",
                        "Simulator rejected charge because the car was not yet waiting at exit.",
                        "Reset to WAITING_TO_CHARGE; next bounded backoff attempt is now safe."
                    )
                    schedule_charge_attempt(
                        penalty_plate,
                        float(car.get("parking_cost") or 0.0),
                        float(car.get("charging_cost") or 0.0),
                        trigger="explicit not-waiting penalty",
                    )

            # If this is explicitly an unpaid escape, preserve that status.
            if penalty_plate and ("unpaid" in lower_reason or ("escape" in lower_reason and "pay" in lower_reason)):
                car = get_car(penalty_plate)
                upsert_car(
                    penalty_plate,
                    status="ESCAPED_UNPAID",
                    departure_time=(car or {}).get("departure_time") or penalty_time
                )

            log_penalty(reason, fine, data, penalty_plate)
            record_alert(
                alert_key=f"PENALTY:{data.get('EventId') or data.get('SequenceId') or datetime.now().timestamp()}",
                alert_type="PENALTY",
                severity="CRITICAL",
                plate=penalty_plate,
                reason=str(reason or "Simulator penalty"),
                event_time=penalty_time,
                fine_amount=float(fine or 0)
            )
            log_decision(
                penalty_plate, "PENALTY",
                f"{reason} | fine={fine}",
                "Stored in penalties + operational alerts for immediate dashboard visibility."
            )

        elif event_class == "carbon_monoxide_event":
            zone = str(data.get("ZoneName") or data.get("Zone") or "")
            level = data.get("CarbonMonoxideLevel")
            danger = data.get("DangerLevel") or data.get("Risk")
            with state_lock:
                if zone:
                    zones.setdefault(zone, {"name": zone})
                    if level is not None:
                        zones[zone]["gasCarbonMonoxideLevel"] = level
                    if danger is not None:
                        zones[zone]["risk"] = danger
            log_decision("", "CO_EVENT", f"zone={zone} level={level} risk={danger}",
                         "Live zone state came from simulator webhook.")
            if zone:
                threading.Thread(target=apply_co_policy, args=(zone,), daemon=True).start()

        elif event_class in ("fan_action", "exhaust_fan_action", "exhaust_fan_state"):
            name = str(data.get("Name") or "")
            action = str(data.get("Action") or "").lower()
            value = data.get("IsOn")
            if value is None and action in ("on", "off"):
                value = action == "on"
            if name in fans and value is not None:
                with state_lock:
                    _set_local_component_state("fan", name, bool(value))

        elif event_class in ("light_action", "light_state", "light_state_changed"):
            name = str(data.get("Name") or "")
            action = str(data.get("Action") or "").lower()
            value = data.get("IsOn")
            if value is None and action in ("on", "off"):
                value = action == "on"
            if name in lights and value is not None:
                with state_lock:
                    _set_local_component_state("light", name, bool(value))

        # Every simulator webhook carries/refreshes simulator time. Enforce the
        # day/night rule only when a state change is actually needed.
        if server_clock and lights:
            threading.Thread(target=apply_light_policy, args=(server_clock,), daemon=True).start()

    except Exception as e:
        print("[EVENT ERROR]", e)
        log_decision("", "ERROR", f"{event_class}: {e}")


# =====================================================================
# WEBHOOK ENDPOINT (with signature + sequence checks)
# =====================================================================
@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(silent=True) or {}
    print("\n[WEBHOOK]", data)

    # LEVEL-2 SECURITY INVARIANT:
    # only correctly signed calls are allowed to reach handle_event().
    sig_result = verify_webhook_signature(data)

    if sig_result is None:
        with state_lock:
            stats["webhooks_unsigned_rejected"] += 1
        try:
            log_unsigned_webhook(data)
        except Exception as e:
            print("[WEBHOOK] unsigned log failure:", e)
        log_decision("", "WEBHOOK_UNSIGNED_REJECTED",
                     f"event={data.get('EventClass')} seq={data.get('SequenceId')}",
                     "Level 2 accepts only signed webhooks; event was logged but NOT acted on.")
        log_audit("WEBHOOK_UNSIGNED_REJECTED", str(data.get("EventClass") or ""),
                  f"seq={data.get('SequenceId')}", "REJECTED", actor="SYSTEM", role="SYSTEM")
        return jsonify({"status": "unsigned_logged_rejected"}), 403

    if sig_result is False:
        with state_lock:
            stats["webhooks_rejected_sig"] += 1
        log_decision("", "WEBHOOK_REJECTED", "Signature mismatch",
                     "Integrity check failed; event was not acted on.")
        log_audit("WEBHOOK_SIGNATURE_REJECTED", str(data.get("EventClass") or ""),
                  f"seq={data.get('SequenceId')}", "REJECTED", actor="SYSTEM", role="SYSTEM")
        return jsonify({"status": "signature_invalid"}), 403

    with state_lock:
        stats["webhooks_verified"] += 1
    signature_label = "verified"

    # Sequence monitoring applies only after signature authentication.
    check_sequence_gap(data.get("SequenceId"))

    # EventId dedup / idempotency.
    if data.get("EventId") and not save_event(data, verified=(sig_result is True)):
        print("[WEBHOOK] Duplicate EventId ignored.")
        return jsonify({"status": "duplicate_ignored"}), 200

    threading.Thread(target=handle_event, args=(data,), daemon=True).start()
    return jsonify({"status": "received", "signature": signature_label}), 200


# =====================================================================
# WEB DASHBOARD + ROLES
# =====================================================================
MAINTENANCE_USER = os.getenv("PARKMIND_MAINT_USER", "maintenance")

# Keep the familiar hackathon defaults so the current workflow still starts,
# while allowing strong credentials to be supplied without editing source code.
_ADMIN_PASSWORD = os.getenv("PARKMIND_ADMIN_PASSWORD", "admin")
_OPERATOR_PASSWORD = os.getenv("PARKMIND_OPERATOR_PASSWORD", "operator")
_MAINTENANCE_PASSWORD = os.getenv("PARKMIND_MAINT_PASSWORD", "maintenance")

USERS = {
    "admin": {"password_hash": _hash_password(_ADMIN_PASSWORD), "role": "Admin"},
    "operator": {"password_hash": _hash_password(_OPERATOR_PASSWORD), "role": "Operator"},
    MAINTENANCE_USER: {"password_hash": _hash_password(_MAINTENANCE_PASSWORD), "role": "Maintenance"},
}

# Unknown usernames still perform a real PBKDF2 verification to reduce account
# enumeration timing differences.  Plaintext variables are then discarded.
DUMMY_PASSWORD_HASH = _hash_password(secrets.token_urlsafe(32))
del _ADMIN_PASSWORD, _OPERATOR_PASSWORD, _MAINTENANCE_PASSWORD

LOGIN_HTML = """
<!doctype html>
<title>PARKMIND v2 — Login</title>
<style>
body{font-family:'Segoe UI',Arial;background:#0f172a;color:#e5e7eb;display:grid;place-items:center;height:100vh;margin:0}
.card{background:#1e293b;padding:36px;border-radius:18px;width:340px;box-shadow:0 20px 60px rgba(0,0,0,0.4)}
h1{margin:0 0 4px;font-size:28px;background:linear-gradient(90deg,#22d3ee,#a78bfa);-webkit-background-clip:text;-webkit-text-fill-color:transparent}
input,button{width:100%;box-sizing:border-box;padding:12px;margin:8px 0;border-radius:10px;border:1px solid #334155;background:#0f172a;color:#fff}
button{font-weight:bold;cursor:pointer;background:linear-gradient(90deg,#0ea5e9,#7c3aed);border:0}
button:hover{filter:brightness(1.15)}
.err{color:#fca5a5;font-size:14px}
.sub{opacity:.6;font-size:13px;margin-top:8px}
.badge{display:inline-block;background:#0f172a;color:#22d3ee;padding:4px 10px;border-radius:8px;font-size:11px;margin:2px}
</style>
<div class="card">
<h1>PARKMIND v2</h1>
<p class="sub">Pretty Little Hackers — Level 2 Secure Control Center</p>
<div style="margin:10px 0">
<span class="badge">Signature Verified</span>
<span class="badge">Sequence Gap Detection</span>
<span class="badge">Multi-Factor Payment</span>
<span class="badge">Smart Spot AI</span>
<span class="badge">Maintenance Control</span>
<span class="badge">5-Attempt Lockout</span>
<span class="badge">PBKDF2 Password Hashing</span>
</div>
{% if error %}<p class="err">{{error}}</p>{% endif %}
<form method="post">
<input name="username" placeholder="Username" required>
<input name="password" type="password" placeholder="Password" required>
<button>Login</button>
</form>
<p class="sub">Secure authentication · failed attempts are audited and rate-limited.</p>
</div>
"""

OPERATOR_LOGIN_HTML = """
<!doctype html>
<title>PARKMIND — Operator Login</title>
<style>
body{font-family:'Segoe UI',Arial;background:#0b1120;color:#e5e7eb;display:grid;place-items:center;min-height:100vh;margin:0}
.card{width:340px;background:#111827;border:1px solid #263244;border-radius:16px;padding:28px}
h1{margin:0 0 4px;color:#67e8f9}.sub{font-size:12px;color:#94a3b8;margin-bottom:16px}
input,button{width:100%;box-sizing:border-box;padding:11px;margin:6px 0;border-radius:8px}
input{background:#0b1120;color:#fff;border:1px solid #334155}
button{border:0;background:#0284c7;color:#fff;font-weight:700;cursor:pointer}
.err{color:#fca5a5;font-size:12px}a{color:#67e8f9;font-size:12px}
</style>
<div class="card">
<h1>Operator Login</h1>
<div class="sub">Live parking operations and incident response</div>
{% if error %}<div class="err">{{error}}</div>{% endif %}
<form method="post">
<input name="username" value="operator" readonly>
<input name="password" type="password" placeholder="Operator password" required autofocus>
<button>Enter Operator Dashboard</button>
</form>
<p><a href="/login">Admin / standard login</a></p>
</div>
"""


ADMIN_DASH_HTML = r'''

<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">

<title>PARKMIND — Admin Command Centre</title>

<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>

<style>

*{
    box-sizing:border-box;
}

body{
    margin:0;
    background:
        radial-gradient(circle at top right,#102a43 0,#07111f 38%,#050b14 100%);
    color:#e5e7eb;
    font-family:'Segoe UI',Arial,sans-serif;
}

header{
    min-height:70px;
    padding:12px 24px;
    background:rgba(9,20,35,.95);
    border-bottom:1px solid #26364e;
    display:flex;
    align-items:center;
    justify-content:space-between;
    gap:20px;
    position:sticky;
    top:0;
    z-index:20;
    backdrop-filter:blur(12px);
}

.brand{
    font-size:23px;
    font-weight:900;
    color:#67e8f9;
    letter-spacing:.5px;
}

.subtitle{
    font-size:10px;
    color:#94a3b8;
    margin-top:3px;
}

a{
    color:#67e8f9;
    text-decoration:none;
}

.nav{
    display:flex;
    gap:7px;
    align-items:center;
    flex-wrap:wrap;
}

.nav a,
.nav button{
    background:#0f2238;
    border:1px solid #29425e;
    color:#ccefff;
    padding:7px 10px;
    border-radius:8px;
    font-size:11px;
    cursor:pointer;
}

.nav a:hover,
.nav button:hover{
    background:#123452;
}

.page{
    max-width:1550px;
    margin:auto;
    padding:18px;
}

.live{
    display:flex;
    justify-content:space-between;
    align-items:center;
    background:linear-gradient(90deg,#082438,#0c1c30);
    border:1px solid #1c5571;
    padding:10px 14px;
    border-radius:12px;
    margin-bottom:12px;
}

.live-dot{
    display:inline-block;
    width:9px;
    height:9px;
    background:#4ade80;
    border-radius:50%;
    margin-right:7px;
    box-shadow:0 0 12px #4ade80;
}

.kpis{
    display:grid;
    grid-template-columns:repeat(7,1fr);
    gap:9px;
    margin-bottom:12px;
}

.kpi{
    background:linear-gradient(145deg,#101d31,#0d1828);
    border:1px solid #26364e;
    border-radius:14px;
    padding:13px;
    min-height:90px;
}

.kpi:hover{
    transform:translateY(-2px);
    border-color:#2d6684;
    transition:.2s;
}

.kpi .v{
    font-size:24px;
    font-weight:900;
    margin-bottom:5px;
}

.kpi .k{
    font-size:9px;
    color:#94a3b8;
    text-transform:uppercase;
    letter-spacing:.5px;
}

.good{color:#86efac}
.cyan{color:#67e8f9}
.warn{color:#fbbf24}
.bad{color:#f87171}
.purple{color:#c4b5fd}

.grid{
    display:grid;
    grid-template-columns:1.45fr .55fr;
    gap:12px;
}

.two{
    display:grid;
    grid-template-columns:1fr 1fr;
    gap:12px;
    margin-top:12px;
}

.card{
    background:rgba(16,27,45,.92);
    border:1px solid #26364e;
    border-radius:15px;
    padding:15px;
    box-shadow:0 10px 30px rgba(0,0,0,.15);
}

.card h2{
    margin:0 0 12px;
    font-size:13px;
    text-transform:uppercase;
    color:#dbeafe;
    letter-spacing:.5px;
}

.card-head{
    display:flex;
    justify-content:space-between;
    align-items:center;
    gap:10px;
}

.mini{
    font-size:10px;
    color:#94a3b8;
}

.chart-box{
    height:270px;
    position:relative;
}

.chart-box.small{
    height:220px;
}

canvas{
    max-width:100%;
}

table{
    width:100%;
    border-collapse:collapse;
    font-size:11px;
}

th,td{
    padding:8px;
    border-bottom:1px solid #26364e;
    text-align:left;
}

th{
    font-size:9px;
    color:#94a3b8;
    text-transform:uppercase;
}

.badge{
    padding:3px 7px;
    border-radius:999px;
    background:#17263a;
    font-size:9px;
}

.section-title{
    margin:20px 0 10px;
    font-size:12px;
    color:#94a3b8;
    text-transform:uppercase;
    letter-spacing:1px;
}

.metric-row{
    display:grid;
    grid-template-columns:1fr 1fr;
    gap:9px;
}

.metric{
    padding:11px;
    border:1px solid #253a54;
    border-radius:10px;
    background:#0c1727;
}

.metric strong{
    display:block;
    font-size:18px;
    margin-top:4px;
}

button{
    border:0;
    background:#0284c7;
    color:white;
    cursor:pointer;
    padding:7px 10px;
    border-radius:7px;
}

button:hover{
    background:#0369a1;
}

.action-btn{
    padding:9px 13px;
    border-radius:9px;
    background:#075985;
    border:1px solid #0e7490;
    color:white;
    cursor:pointer;
    font-weight:700;
}

.action-btn:hover{
    background:#0e7490;
}

.alert-box{
    padding:11px;
    border-radius:10px;
    margin-bottom:8px;
    background:#171b27;
    border:1px solid #30384a;
}

.alert-box.critical{
    border-left:4px solid #f87171;
}

.alert-box.warning{
    border-left:4px solid #fbbf24;
}

.maintenance-highlight{
    border:1px solid #735d18;
    background:linear-gradient(135deg,#241e0b,#16150d);
}

.progress{
    height:7px;
    background:#1e293b;
    border-radius:99px;
    overflow:hidden;
    margin-top:7px;
}

.progress span{
    display:block;
    height:100%;
    background:#fbbf24;
}

.footer-note{
    text-align:center;
    color:#64748b;
    font-size:9px;
    margin:25px 0 10px;
}

@media(max-width:1200px){
    .kpis{
        grid-template-columns:repeat(4,1fr);
    }
}

@media(max-width:900px){
    .kpis,
    .grid,
    .two{
        grid-template-columns:1fr 1fr;
    }
}

@media(max-width:600px){
    .kpis,
    .grid,
    .two{
        grid-template-columns:1fr;
    }

    header{
        position:static;
        flex-direction:column;
        align-items:flex-start;
    }
}


/* ============================================================
   EXISTING PARKMIND LIVE CONTROLS — ADMIN ONLY
   ============================================================ */
.control-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-top:12px}
.control-list{display:flex;flex-direction:column;gap:7px}
.control-row{display:flex;justify-content:space-between;align-items:center;gap:12px;padding:9px 0;border-bottom:1px solid #26364e}
.control-row:last-child{border-bottom:0}
.control-actions{display:flex;gap:4px;flex-wrap:wrap;justify-content:flex-end}
.control-actions form{display:inline}
.btn-green{background:#047857}.btn-red{background:#b91c1c}.btn-gray{background:#475569}.btn-yellow{background:#a16207}
.btn-green:hover{background:#059669}.btn-red:hover{background:#dc2626}.btn-gray:hover{background:#64748b}.btn-yellow:hover{background:#ca8a04}
.override-open{color:#86efac}.override-closed{color:#fca5a5}.override-auto{color:#94a3b8}
.live-table-wrap{max-height:330px;overflow:auto}
.alert-row{padding:10px;border-radius:9px;background:#281116;border:1px solid #7f1d1d;margin-bottom:7px;font-size:10px;line-height:1.5}
.alert-row.high{background:#2a210d;border-color:#92400e}
.admin-tool-row{display:flex;gap:6px;flex-wrap:wrap;align-items:center}
.zone-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(165px,1fr));gap:8px}
.zone-box{padding:10px;border:1px solid #253a54;border-radius:10px;background:#0c1727}
.zone-bar{height:6px;background:#1e293b;border-radius:99px;overflow:hidden;margin-top:7px}.zone-bar span{display:block;height:100%;background:#38bdf8}
@media(max-width:900px){.control-grid{grid-template-columns:1fr}}

</style>
</head>

<body>

<header>

<div>
    <div class="brand">PARKMIND · ADMIN COMMAND CENTRE</div>
    <div class="subtitle">
        Business intelligence · Parking operations · Maintenance · Security
    </div>
</div>

<div class="nav">
    <span class="mini">{{sim_time or 'Simulator time unavailable'}}</span>
    <a href="#live-control">Live Controls</a>
    <a href="/maintenance/">Maintenance</a>
    <a href="/penalties">Penalties</a>
    <a href="/reports/daily">Daily Report</a>
    <a href="#critical-alerts">Alerts</a>
    <a href="/export/cars">Export</a>
    <a href="/audit">Audit</a>
    <a href="/search">Search</a>
    <a href="/logout">Logout</a>
</div>

</header>


<div class="page">

{% if recent_logins %}
<div class="card" style="margin-bottom:12px;border-color:#365a73">
<div class="card-head"><h2>Login Security · Last 3 Attempts</h2><span class="badge">LEVEL 2 RBAC</span></div>
<div class="mini">
{% for x in recent_logins %}
<b class="{{'good' if x.success else 'bad'}}">{{'SUCCESS' if x.success else 'FAILED'}}</b>
{{x.username or '-'}} · {{x.attempted_at}} · {{x.ip or '-'}}{% if not loop.last %}<br>{% endif %}
{% endfor %}
</div>
</div>
{% endif %}

<div class="live">
    <div>
        <span class="live-dot"></span>
        <b>LIVE SIMULATOR MONITORING</b>
        <span class="mini"> · Dashboard refreshes every 10 seconds</span>
    </div>

    <div class="mini">
        Simulator date: <b>{{today_date or 'Waiting...'}}</b>
    </div>
</div>


<!-- =====================================================
     KPI STRIP
====================================================== -->

<div class="kpis">

<div class="kpi">
    <div class="v good">{{'%.2f'|format(today.revenue)}}</div>
    <div class="k">Revenue Today</div>
</div>

<div class="kpi">
    <div class="v cyan">{{today.paid_cars}}</div>
    <div class="k">Paid Cars</div>
</div>

<div class="kpi">
    <div class="v warn">{{'%.2f'|format(today.avg_ticket)}}</div>
    <div class="k">Average Ticket</div>
</div>

<div class="kpi">
    <div class="v {{'good' if delta is not none and delta>=0 else 'bad'}}">
        {{delta_text}}
    </div>
    <div class="k">vs Yesterday</div>
</div>

<div class="kpi">
    <div class="v bad">
        {{'%.2f'|format(maintenance.today_cost)}}
    </div>
    <div class="k">Maintenance Cost</div>
</div>

<div class="kpi">
    <div class="v good">
        {{'%.2f'|format(maintenance.revenue_after_maintenance)}}
    </div>
    <div class="k">Revenue After Maintenance</div>
</div>

<div class="kpi">
    <div class="v {{'bad' if overview.alerts else 'good'}}">
        {{overview.alerts}}
    </div>
    <div class="k">Critical Alerts</div>
</div>

</div>


<!-- =====================================================
     REVENUE ANALYTICS
====================================================== -->

<div class="section-title">Revenue Intelligence</div>

<div class="grid">

<div class="card">

<div class="card-head">
    <h2>7-Day Revenue Trend</h2>
    <span class="badge">SIMULATOR DATA</span>
</div>

<div class="chart-box">
    <canvas id="revenueChart"></canvas>
</div>

</div>


<div class="card">

<h2>Today's Revenue Mix</h2>

<div class="chart-box">
    <canvas id="mixChart"></canvas>
</div>

<div class="metric-row">

<div class="metric">
    <span class="mini">Parking</span>
    <strong class="cyan">
        {{'%.2f'|format(mix.parking)}}
    </strong>
</div>

<div class="metric">
    <span class="mini">Charging</span>
    <strong class="purple">
        {{'%.2f'|format(mix.charging)}}
    </strong>
</div>

</div>

</div>

</div>


<div class="two">

<div class="card">

<h2>Hourly Revenue</h2>

<div class="chart-box small">
    <canvas id="hourlyChart"></canvas>
</div>

</div>


<div class="card">

<h2>Today vs Yesterday</h2>

<div class="chart-box small">
    <canvas id="comparisonChart"></canvas>
</div>

</div>

</div>


<!-- =====================================================
     OPERATIONS
====================================================== -->

<div class="section-title">Parking Operations</div>

<div class="two">

<div class="card">

<div class="card-head">
    <h2>Parking Operations</h2>
    <a href="#live-control" class="action-btn">
        Open Live Controls →
    </a>
</div>

<div class="metric-row">

<div class="metric">
    <span class="mini">Arrivals Today</span>
    <strong class="cyan">{{operations.arrivals}}</strong>
</div>

<div class="metric">
    <span class="mini">Departures Today</span>
    <strong class="good">{{operations.departures}}</strong>
</div>

<div class="metric">
    <span class="mini">Penalties</span>
    <strong class="bad">{{operations.penalties}}</strong>
</div>

<div class="metric">
    <span class="mini">CO Incidents</span>
    <strong class="warn">{{operations.co_incidents}}</strong>
</div>

</div>

</div>


<div class="card">

<h2>System Health</h2>

<div class="metric-row">

<div class="metric">
    <span class="mini">Broken Components</span>
    <strong class="bad">{{overview.broken}}</strong>
</div>

<div class="metric">
    <span class="mini">Maintenance Due</span>
    <strong class="warn">{{overview.maintenance_due}}</strong>
</div>

<div class="metric">
    <span class="mini">Verified Webhooks</span>
    <strong class="good">{{overview.events}}</strong>
</div>

<div class="metric">
    <span class="mini">Failed Logins</span>
    <strong class="bad">{{overview.failed_logins}}</strong>
</div>

</div>

<form method="post" action="/sync" style="margin-top:12px">
    <button>↻ Sync Simulator Topology</button>
</form>

</div>

</div>


<!-- =====================================================
     MAINTENANCE
====================================================== -->

<div class="section-title">Maintenance Intelligence</div>

<div class="two">

<div class="card maintenance-highlight">

<h2>Actual Maintenance Economics</h2>

<div class="metric-row">

<div class="metric">
    <span class="mini">Preventive Jobs</span>
    <strong>{{maintenance.preventive_jobs}}</strong>
</div>

<div class="metric">
    <span class="mini">Preventive Cost</span>
    <strong class="warn">
        {{'%.2f'|format(maintenance.preventive_cost)}}
    </strong>
</div>

<div class="metric">
    <span class="mini">Corrective Jobs</span>
    <strong>{{maintenance.corrective_jobs}}</strong>
</div>

<div class="metric">
    <span class="mini">Corrective Cost</span>
    <strong class="bad">
        {{'%.2f'|format(maintenance.corrective_cost)}}
    </strong>
</div>

</div>

<p class="mini" style="margin-top:12px">
Costs shown here are only simulator-confirmed
<code>RepairCost</code> values from completed repair events.
</p>

</div>


<div class="card">

<h2>Maintenance Cost Breakdown</h2>

<div class="chart-box small">
    <canvas id="maintenanceChart"></canvas>
</div>

</div>

</div>


<div class="card" style="margin-top:12px">

<div class="card-head">
<h2>Recent Maintenance Jobs</h2>
<a href="/maintenance/">View all →</a>
</div>

<table>

<tr>
<th>Completed</th>
<th>Component</th>
<th>Type</th>
<th>Duration</th>
<th>Actual Cost</th>
</tr>

{% for m in maintenance.recent %}

<tr>

<td>{{m.completed_simulator_time or '-'}}</td>

<td>
    <b>{{m.component or '-'}}</b>
    <div class="mini">{{m.kind or ''}}</div>
</td>

<td>
    {{m.repair_type or '-'}}
</td>

<td>
    {{m.duration}}
</td>

<td class="good">

{% if m.repair_cost is not none %}
    <b>{{'%.2f'|format(m.repair_cost)}}</b>
{% else %}
    Pending
{% endif %}

</td>

</tr>

{% else %}

<tr>
<td colspan="5">No repair jobs recorded yet.</td>
</tr>

{% endfor %}

</table>

</div>


<!-- =====================================================
     PAYMENTS
====================================================== -->

<div class="section-title">Financial Transactions</div>

<div class="two">

<div class="card">

<h2>Recent Accepted Payments</h2>

<table>

<tr>
<th>Simulator Time</th>
<th>Plate</th>
<th>Parking</th>
<th>Charging</th>
<th>Total</th>
</tr>

{% for p in payments %}

<tr>

<td>{{p.payment_time}}</td>

<td><b>{{p.plate}}</b></td>

<td>{{'%.2f'|format(p.parking_cost or 0)}}</td>

<td>{{'%.2f'|format(p.charging_cost or 0)}}</td>

<td class="good">
<b>{{'%.2f'|format(p.actual_paid or 0)}}</b>
</td>

</tr>

{% else %}

<tr>
<td colspan="5">No accepted payments yet.</td>
</tr>

{% endfor %}

</table>

</div>


<div class="card">

<h2>Login Security</h2>

<table>

<tr>
<th>Time</th>
<th>Name</th>
<th>Result</th>
<th>IP</th>
</tr>

{% for x in logins %}

<tr>

<td>{{x.attempted_at}}</td>

<td>{{x.username}}</td>

<td class="{{'good' if x.success else 'bad'}}">
{{'SUCCESS' if x.success else 'FAILED'}}
</td>

<td>{{x.ip}}</td>

</tr>

{% else %}

<tr>
<td colspan="4">No login attempts.</td>
</tr>

{% endfor %}

</table>

</div>

</div>



<!-- =====================================================
     EXISTING LIVE PARKING CONTROLS — ADMIN ONLY
====================================================== -->
<div id="live-control" class="section-title">Live Parking Control & Overrides</div>

<div class="control-grid">

<div class="card">
<div class="card-head">
<h2>Gate Manual Overrides</h2>
<span class="badge">OPEN / CLOSE / AUTO</span>
</div>
<div class="control-list">
{% for g in gate_rows %}
<div class="control-row">
<div>
<b>{{g.name}}</b>
<div class="mini">{{g.zone}} · {{g.state}} · health {{g.health}}% · override
<span class="{{'override-open' if g.override == 'OPEN' else 'override-closed' if g.override == 'CLOSED' else 'override-auto'}}"><b>{{g.override}}</b></span>
{% if g.name == entry_gate %} · ENTRY SPAWN PROTECTED{% endif %}
{% if g.broken %} · BROKEN{% endif %}{% if g.maintenance %} · MAINTENANCE{% endif %}
</div>
</div>
<div class="control-actions">
<form method="post" action="/gate/{{g.name}}/open"><button class="btn-green">Open + Hold</button></form>
<form method="post" action="/gate/{{g.name}}/close"><button class="btn-red" {% if g.name == entry_gate %}disabled title="Entry gate is spawn-path protected"{% endif %}>Close + Hold</button></form>
<form method="post" action="/gate/{{g.name}}/auto"><button class="btn-gray">Auto</button></form>
</div>
</div>
{% else %}
<div class="mini">No gates discovered yet.</div>
{% endfor %}
</div>
</div>

<div class="card">
<div class="card-head">
<h2>Zone Occupancy</h2>
<span class="badge">LIVE API CACHE</span>
</div>
<div class="zone-grid">
{% for z in zone_rows %}
<div class="zone-box">
<b>{{z.name}}</b>
<div class="mini">{{z.occupied}} occupied · {{z.free}} free · {{z.reserved}} reserved</div>
<div class="mini">CO {{z.co if z.co is not none else '-'}} · {{z.risk}}</div>
<div class="zone-bar"><span style="width:{{z.percent}}%"></span></div>
</div>
{% else %}
<div class="mini">No zones discovered yet.</div>
{% endfor %}
</div>
</div>

</div>

<div class="control-grid">

<div class="card">
<div class="card-head">
<h2>Active / Recent Vehicles</h2>
<a href="/search">Full Search →</a>
</div>
<div class="live-table-wrap">
<table>
<tr><th>Plate</th><th>Type</th><th>Status</th><th>Spot</th><th>Stay</th><th>Payment</th><th>Actions</th></tr>
{% for c in cars %}
<tr>
<td><b>{{c.plate}}</b></td>
<td>{{c.car_type or '-'}}</td>
<td>{{c.status}}</td>
<td>{{c.assigned_spot or c.actual_spot or '-'}}</td>
<td>{{c.display_stay}}</td>
<td>{{c.payment_status}}</td>
<td>
<div class="control-actions">
{% if c.status not in ('LEFT','ESCAPED_UNPAID') %}
<form method="post" action="/car/{{c.plate}}/exit"><button class="btn-yellow">Exit</button></form>
<form method="post" action="/car/{{c.plate}}/reassign"><button class="btn-gray">Reassign</button></form>
{% endif %}
{% if c.status in ('AT_EXIT','PAYMENT_PENDING','PAYMENT_HOLD') %}
<form method="post" action="/car/{{c.plate}}/retry-payment"><button>Retry Pay</button></form>
{% endif %}
</div>
</td>
</tr>
{% else %}
<tr><td colspan="7">No vehicles recorded yet.</td></tr>
{% endfor %}
</table>
</div>
</div>

<div id="critical-alerts" class="card">
<div class="card-head">
<h2>Critical Alerts</h2>
<form method="post" action="/admin/reconcile-alerts"><button class="btn-gray">Resolve Alert Plates</button></form>
</div>
<div style="max-height:330px;overflow:auto">
{% for a in alerts %}
<div class="alert-row {{'high' if a.severity == 'HIGH' else ''}}">
<b>{{a.severity or 'ALERT'}} · {{a.alert_type or '-'}}</b>
<div>{{a.plate or 'Unresolved plate'}} · {{a.reason or '-'}}</div>
<div class="mini">Entry {{a.display_entry}} · Stay {{a.display_stay}} · Stuck {{a.display_stuck}}</div>
</div>
{% else %}
<div class="mini">No alerts recorded.</div>
{% endfor %}
</div>
</div>

</div>

<div class="card" style="margin-top:12px">
<div class="card-head"><h2>Manual / Unregistered Vehicle Recovery</h2><span class="badge">LEVEL 2 EXCEPTION FLOW</span></div>
<div class="mini" style="margin-bottom:10px">Use only when a real vehicle entered/parked without normal entry or spot events. This creates an audited tracked record so the normal payment-locked exit workflow can be used.</div>
<form method="post" action="/admin/manual-car/recover" style="display:flex;gap:7px;flex-wrap:wrap;align-items:center">
<input name="plate" required placeholder="Plate e.g. ABC 123" style="padding:7px;border-radius:7px;border:1px solid #334155;background:#0c1727;color:white">
<select name="car_type" style="padding:7px;border-radius:7px;background:#0c1727;color:white;border:1px solid #334155">
<option>Normal</option><option>Electric</option><option>Accessible</option>
</select>
<select name="spot" style="padding:7px;border-radius:7px;background:#0c1727;color:white;border:1px solid #334155">
<option value="">Already at ExitSpot / unknown bay</option>
{% for sp in spot_rows %}<option value="{{sp.name}}">{{sp.name}} · {{sp.zone}} · {{sp.type}}</option>{% endfor %}
</select>
<input name="estimated_minutes" type="number" min="1" max="1440" value="1" required title="Estimated parked duration in minutes" style="width:95px;padding:7px;border-radius:7px;border:1px solid #334155;background:#0c1727;color:white">
<label class="mini"><input type="checkbox" name="start_exit" value="1" checked> start normal exit flow</label>
<button class="btn-yellow">Recover Vehicle</button>
</form>
</div>

<div class="card" style="margin-top:12px">
<div class="card-head"><h2>Administrative Actions</h2><span class="badge">EXISTING PARKMIND FUNCTIONS</span></div>
<div class="admin-tool-row">
<form method="post" action="/sync"><button>↻ Sync Simulator Topology</button></form>
<form method="post" action="/admin/test-webhook"><button>Test Webhook</button></form>
<form method="post" action="/admin/reconcile-alerts"><button class="btn-gray">Resolve Alert Plates</button></form>
<form method="post" action="/admin/demo/full"><button>Full Lot Demo</button></form>
<form method="post" action="/admin/demo/fraud"><button>Payment Fraud Demo</button></form>
<form method="post" action="/admin/demo/broken_gate"><button>Broken Gate Demo</button></form>
<form method="post" action="/admin/demo/live_full"><button class="btn-red">LIVE Full Next Car</button></form>
<form method="post" action="/admin/demo/clear"><button class="btn-gray">Clear Demo</button></form>
<form method="post" action="/admin/demo/toggle"><button class="btn-yellow">Toggle Judge Demo</button></form>
</div>
<div class="mini" style="margin-top:10px">Entry gate: {{entry_gate or '-'}} · Exit gate: {{exit_gate or '-'}} · Last discovery sync: {{last_sync or '-'}} · Judge demo: {{'ON' if demo.enabled else 'OFF'}}</div>
</div>


<div class="footer-note">
PARKMIND Level 2 · All operational figures are derived from simulator/database events.
</div>

</div>


<script>

const chartFont = {
    family: "'Segoe UI', Arial",
    size: 11
};

Chart.defaults.color = "#94a3b8";
Chart.defaults.font.family = "'Segoe UI', Arial";


// ========================================================
// 7 DAY REVENUE
// ========================================================

new Chart(
    document.getElementById("revenueChart"),
    {
        type:"bar",

        data:{
            labels: {{daily_labels|safe}},

            datasets:[{
                label:"Revenue",
                data: {{daily_values|safe}},
                borderRadius:7,
                backgroundColor:"#0ea5e9",
                hoverBackgroundColor:"#38bdf8"
            }]
        },

        options:{
            responsive:true,
            maintainAspectRatio:false,

            plugins:{
                legend:{
                    display:false
                },

                tooltip:{
                    callbacks:{
                        label:function(context){
                            return " Revenue: " +
                                Number(context.raw).toFixed(2);
                        }
                    }
                }
            },

            scales:{
                x:{
                    grid:{
                        display:false
                    }
                },

                y:{
                    beginAtZero:true,
                    grid:{
                        color:"rgba(148,163,184,.08)"
                    }
                }
            }
        }
    }
);


// ========================================================
// REVENUE MIX
// ========================================================

new Chart(
    document.getElementById("mixChart"),
    {
        type:"doughnut",

        data:{
            labels:["Parking","Charging"],

            datasets:[{
                data:[
                    {{mix.parking}},
                    {{mix.charging}}
                ],

                backgroundColor:[
                    "#38bdf8",
                    "#a78bfa"
                ],

                borderColor:"#101b2d",
                borderWidth:4
            }]
        },

        options:{
            responsive:true,
            maintainAspectRatio:false,

            cutout:"65%",

            plugins:{
                legend:{
                    position:"bottom"
                }
            }
        }
    }
);


// ========================================================
// HOURLY
// ========================================================

new Chart(
    document.getElementById("hourlyChart"),
    {
        type:"line",

        data:{
            labels: {{hour_labels|safe}},

            datasets:[{
                label:"Revenue",
                data: {{hour_values|safe}},
                borderColor:"#22d3ee",
                backgroundColor:"rgba(34,211,238,.10)",
                fill:true,
                tension:.35,
                pointRadius:2
            }]
        },

        options:{
            responsive:true,
            maintainAspectRatio:false,

            plugins:{
                legend:{
                    display:false
                }
            },

            scales:{
                x:{
                    grid:{
                        display:false
                    }
                },

                y:{
                    beginAtZero:true,
                    grid:{
                        color:"rgba(148,163,184,.08)"
                    }
                }
            }
        }
    }
);


// ========================================================
// TODAY VS YESTERDAY
// ========================================================

new Chart(
    document.getElementById("comparisonChart"),
    {
        type:"bar",

        data:{
            labels:["Yesterday","Today"],

            datasets:[{
                data:[
                    {{yesterday.revenue}},
                    {{today.revenue}}
                ],

                backgroundColor:[
                    "#64748b",
                    "#22c55e"
                ],

                borderRadius:8
            }]
        },

        options:{
            responsive:true,
            maintainAspectRatio:false,

            plugins:{
                legend:{
                    display:false
                }
            },

            scales:{
                x:{
                    grid:{
                        display:false
                    }
                },

                y:{
                    beginAtZero:true,
                    grid:{
                        color:"rgba(148,163,184,.08)"
                    }
                }
            }
        }
    }
);


// ========================================================
// MAINTENANCE COST
// ========================================================

new Chart(
    document.getElementById("maintenanceChart"),
    {
        type:"doughnut",

        data:{
            labels:["Preventive","Corrective"],

            datasets:[{
                data:[
                    {{maintenance.preventive_cost}},
                    {{maintenance.corrective_cost}}
                ],

                backgroundColor:[
                    "#fbbf24",
                    "#f87171"
                ],

                borderColor:"#101b2d",
                borderWidth:4
            }]
        },

        options:{
            responsive:true,
            maintainAspectRatio:false,

            cutout:"62%",

            plugins:{
                legend:{
                    position:"bottom"
                }
            }
        }
    }
);


// ========================================================
// AUTO REFRESH
// ========================================================

setTimeout(function(){
    window.location.reload();
},10000);

</script>

</body>
</html>

'''


def _admin_dashboard_context(spot_rows, gate_rows, fan_rows, zone_rows, alerts, snapshot_stats):
    """Build the Admin Command Centre analytics from the existing PARKMIND DB/state."""
    conn = db()

    sim_time = str(last_simulator_time or "")
    today_date = sim_time[:10] if len(sim_time) >= 10 else datetime.now().strftime("%Y-%m-%d")
    try:
        today_dt = datetime.strptime(today_date, "%Y-%m-%d").date()
        yesterday_date = (today_dt - timedelta(days=1)).isoformat()
    except Exception:
        today_dt = datetime.now().date()
        today_date = today_dt.isoformat()
        yesterday_date = (today_dt - timedelta(days=1)).isoformat()

    time_expr = "COALESCE(departure_time, exit_arrival_time, parked_time, entry_time, '')"

    def paid_stats(day):
        try:
            row = conn.execute(
                f"""SELECT COALESCE(SUM(actual_paid),0) revenue, COUNT(*) paid_cars
                    FROM cars
                    WHERE payment_status='PAID'
                      AND substr({time_expr},1,10)=?""",
                (day,)
            ).fetchone()
            revenue = float(row["revenue"] or 0)
            paid = int(row["paid_cars"] or 0)
            return {"revenue": revenue, "paid_cars": paid, "avg_ticket": revenue / paid if paid else 0.0}
        except Exception:
            return {"revenue": 0.0, "paid_cars": 0, "avg_ticket": 0.0}

    today = paid_stats(today_date)
    yesterday = paid_stats(yesterday_date)
    delta = None
    if yesterday["revenue"] > 0:
        delta = ((today["revenue"] - yesterday["revenue"]) / yesterday["revenue"]) * 100
    delta_text = "N/A" if delta is None else f"{delta:+.1f}%"

    daily_labels, daily_values = [], []
    for i in range(6, -1, -1):
        ds = (today_dt - timedelta(days=i)).isoformat()
        daily_labels.append(ds[5:])
        daily_values.append(paid_stats(ds)["revenue"])

    try:
        mixrow = conn.execute(
            f"""SELECT COALESCE(SUM(parking_cost),0) p, COALESCE(SUM(charging_cost),0) c
                FROM cars
                WHERE payment_status='PAID'
                  AND substr({time_expr},1,10)=?""",
            (today_date,)
        ).fetchone()
        parking = float(mixrow["p"] or 0)
        charging = float(mixrow["c"] or 0)
    except Exception:
        parking = charging = 0.0
    mix = {"parking": parking, "charging": charging}

    hour_labels = [f"{h:02d}" for h in range(24)]
    hour_values = []
    for hh in hour_labels:
        try:
            row = conn.execute(
                f"""SELECT COALESCE(SUM(actual_paid),0) r
                    FROM cars
                    WHERE payment_status='PAID'
                      AND substr({time_expr},1,10)=?
                      AND substr({time_expr},12,2)=?""",
                (today_date, hh)
            ).fetchone()
            hour_values.append(float(row["r"] or 0))
        except Exception:
            hour_values.append(0.0)

    def sql_count(sql, args=()):
        try:
            return int(conn.execute(sql, args).fetchone()[0] or 0)
        except Exception:
            return 0

    arrivals = sql_count("SELECT COUNT(*) FROM cars WHERE substr(entry_time,1,10)=?", (today_date,))
    departures = sql_count("SELECT COUNT(*) FROM cars WHERE substr(departure_time,1,10)=?", (today_date,))
    verified_events = sql_count(
        "SELECT COUNT(*) FROM events WHERE signature_verified=1 AND substr(server_time,1,10)=?",
        (today_date,)
    )
    co_incidents = sql_count(
        """SELECT COUNT(*) FROM alerts
           WHERE (LOWER(COALESCE(alert_type,'')) LIKE '%co%'
              OR LOWER(COALESCE(reason,'')) LIKE '%carbon monoxide%')
             AND substr(COALESCE(event_time,created_at,''),1,10)=?""",
        (today_date,)
    )

    broken_names = set()
    maintenance_names = set()
    for row in list(spot_rows) + list(gate_rows) + list(fan_rows):
        if row.get("broken"):
            broken_names.add((row.get("name"), row.get("zone", "")))
        if row.get("maintenance") or int(row.get("health") or 100) <= PREVENTIVE_MAINTENANCE_HEALTH:
            maintenance_names.add((row.get("name"), row.get("zone", "")))

    critical_alerts = sum(1 for a in alerts if str(a.get("severity") or "").upper() == "CRITICAL")
    operations = {
        "arrivals": arrivals,
        "departures": departures,
        "penalties": int(snapshot_stats.get("total_penalties", 0) or 0),
        "co_incidents": co_incidents,
    }
    overview = {
        "alerts": critical_alerts,
        "broken": len(broken_names),
        "maintenance_due": len(maintenance_names),
        "events": verified_events,
        "failed_logins": sql_count(
            "SELECT COUNT(*) FROM login_attempts WHERE success=0 AND substr(attempted_at,1,10)=?",
            (today_date,)
        ),
    }

    # Existing maintenance dashboard records job lifecycle, not monetary repair cost.
    # Keep the supplied Admin layout without inventing costs.
    recent_maintenance = []
    preventive_jobs = corrective_jobs = 0
    try:
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        if "maintenance_dashboard_jobs" in tables:
            rows = [dict(r) for r in conn.execute(
                "SELECT * FROM maintenance_dashboard_jobs ORDER BY id DESC LIMIT 8"
            ).fetchall()]
            for m in rows:
                rtype = str(m.get("repair_type") or "").upper()
                if rtype == "PREVENTIVE":
                    preventive_jobs += 1
                elif rtype == "CORRECTIVE":
                    corrective_jobs += 1
                start = m.get("start_sim_time") or m.get("started_at")
                end = m.get("completed_sim_time")
                duration = format_duration(seconds_between(start, end)) if start and end else "-"
                recent_maintenance.append({
                    "completed_simulator_time": end or "-",
                    "component": m.get("component") or "-",
                    "kind": m.get("kind") or "",
                    "repair_type": m.get("repair_type") or "-",
                    "duration": duration,
                    "repair_cost": None,
                })
    except Exception:
        recent_maintenance = []

    maintenance = {
        "today_cost": 0.0,
        "revenue_after_maintenance": today["revenue"],
        "preventive_jobs": preventive_jobs,
        "preventive_cost": 0.0,
        "corrective_jobs": corrective_jobs,
        "corrective_cost": 0.0,
        "recent": recent_maintenance,
    }

    try:
        payments = [dict(r) for r in conn.execute(
            f"""SELECT {time_expr} AS payment_time, plate, parking_cost, charging_cost, actual_paid
                FROM cars
                WHERE payment_status='PAID'
                ORDER BY {time_expr} DESC
                LIMIT 8"""
        ).fetchall()]
    except Exception:
        payments = []

    conn.close()

    return {
        "today_date": today_date,
        "sim_time": sim_time,
        "today": today,
        "yesterday": yesterday,
        "delta": delta,
        "delta_text": delta_text,
        "daily_labels": json.dumps(daily_labels),
        "daily_values": json.dumps(daily_values),
        "mix": mix,
        "hour_labels": json.dumps(hour_labels),
        "hour_values": json.dumps(hour_values),
        "overview": overview,
        "operations": operations,
        "maintenance": maintenance,
        "payments": payments,
        "logins": recent_login_attempts(3),
    }


DASH_HTML = """
<!doctype html>
<html>
<head>
<title>PARKMIND — {{role}} Dashboard</title>
<style>
*{box-sizing:border-box}
body{font-family:'Segoe UI',Arial;margin:0;background:#0b1120;color:#e5e7eb}
header{height:62px;padding:0 18px;background:#111827;border-bottom:1px solid #293548;display:flex;align-items:center;justify-content:space-between;gap:12px}
.brand{font-size:20px;font-weight:800;color:#67e8f9}.sub{font-size:11px;color:#94a3b8}
.role{padding:5px 9px;border-radius:999px;background:#1e293b;color:#cbd5e1;font-size:11px}
a{color:#67e8f9;text-decoration:none}.page{padding:12px;max-width:1500px;margin:auto}
.metrics{display:grid;grid-template-columns:repeat(7,minmax(95px,1fr));gap:8px;margin-bottom:10px}
.metric{background:#111827;border:1px solid #263244;border-radius:10px;padding:9px 10px;min-width:0}
.metric .v{font-size:20px;font-weight:800}.metric .k{font-size:10px;color:#94a3b8;text-transform:uppercase;letter-spacing:.5px}
.danger-v{color:#f87171}.ok-v{color:#86efac}.warn-v{color:#fbbf24}.cyan-v{color:#67e8f9}
.grid{display:grid;grid-template-columns:1.15fr .85fr;gap:10px}
.card{background:#111827;border:1px solid #263244;border-radius:12px;padding:12px;min-width:0}
.card h2{font-size:13px;margin:0 0 9px;text-transform:uppercase;letter-spacing:.7px;color:#cbd5e1}
.alert-card{border-color:#7f1d1d;background:#1c1014}
.alert-title{display:flex;justify-content:space-between;align-items:center;margin-bottom:7px}
.alert-scroll{max-height:215px;overflow:auto}
.alert{display:grid;grid-template-columns:90px 105px 85px 85px 90px 90px 1fr 75px;gap:6px;align-items:center;padding:8px;margin:6px 0;border-radius:8px;background:#2a1217;border-left:4px solid #ef4444;font-size:11px}
.alert.high{border-left-color:#f59e0b;background:#251a10}
.sev{font-weight:800;color:#fca5a5}.plate{font-weight:800;color:#fff}.reason{color:#fecaca;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.mini{font-size:10px;color:#94a3b8}
table{width:100%;border-collapse:collapse;font-size:11px}
th,td{padding:6px;border-bottom:1px solid #263244;text-align:left;white-space:nowrap}
th{font-size:9px;color:#94a3b8;text-transform:uppercase}
.scroll{max-height:260px;overflow:auto}.compact-scroll{max-height:205px;overflow:auto}
.status{font-size:9px;font-weight:700;padding:3px 6px;border-radius:999px;background:#1f2937}
.status-paid{color:#86efac}.status-bad{color:#fca5a5}.status-warn{color:#fbbf24}
button{padding:5px 8px;border:0;border-radius:7px;background:#0284c7;color:#fff;font-size:10px;cursor:pointer;margin:1px}
button.gray{background:#475569}button.danger{background:#b91c1c}button.good{background:#047857}
form{display:inline}.two{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:10px}
.zones{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:7px}
.zone{background:#0b1120;border:1px solid #263244;border-radius:8px;padding:8px}
.zone b{font-size:12px}.bar{height:5px;background:#1e293b;border-radius:4px;overflow:hidden;margin-top:5px}.bar i{display:block;height:100%;background:#38bdf8}
.gate{display:flex;justify-content:space-between;align-items:center;padding:7px 0;border-bottom:1px solid #263244}
.spots{display:grid;grid-template-columns:repeat(10,1fr);gap:4px}
.spot{padding:5px 3px;border-radius:5px;text-align:center;font-size:9px;background:#0f2f25;color:#86efac;border:1px solid #14532d}
.spot.busy{background:#3a171b;color:#fca5a5;border-color:#7f1d1d}.spot.res{background:#3b2a10;color:#fde68a;border-color:#92400e}
details{margin-top:10px;background:#0f172a;border:1px solid #263244;border-radius:9px;padding:8px}
summary{cursor:pointer;color:#cbd5e1;font-size:11px;font-weight:700}
.toolbar{display:flex;gap:6px;align-items:center;flex-wrap:wrap}
.toolbar input{padding:6px 7px;border:1px solid #334155;background:#0b1120;color:#fff;border-radius:7px;font-size:11px}
.empty{padding:14px;text-align:center;color:#64748b;font-size:11px}
.operator-note{background:#082f49;border:1px solid #0e7490;padding:7px 9px;border-radius:8px;font-size:10px;color:#bae6fd;margin-bottom:9px}
@media(max-width:900px){.metrics{grid-template-columns:repeat(3,1fr)}.grid,.two{grid-template-columns:1fr}.spots{grid-template-columns:repeat(6,1fr)}.alert{grid-template-columns:80px 100px 1fr}.alert .optional{display:none}}
</style>
</head>
<body>
<header>
<div><div class="brand">PARKMIND</div><div class="sub">Pretty Little Hackers · Live Parking Control</div></div>
<div class="toolbar">
<span class="role">{{role}} · {{user}}</span>
<a href="/search">Search</a>
{% if is_admin %}<a href="/maintenance/">Maintenance</a><a href="/export/cars">Export</a>{% endif %}
<a href="/logout">Logout</a>
</div>
</header>

<div class="page">

{% if recent_logins %}
<div class="operator-note">
<b>LOGIN SECURITY · LAST 3 ATTEMPTS</b><br>
{% for x in recent_logins %}{{'SUCCESS' if x.success else 'FAILED'}} · {{x.username or '-'}} · {{x.attempted_at}} · {{x.ip or '-'}}{% if not loop.last %}<br>{% endif %}{% endfor %}
</div>
{% endif %}

{% if role == "Operator" %}
<div class="operator-note">
OPERATOR VIEW — live vehicle status, alerts, gate control and recovery actions. Administrative exports, sync and judge tools are hidden.
</div>
{% endif %}

<div class="metrics">
<div class="metric"><div class="v cyan-v">{{capacity.occupied}}/{{capacity.total}}</div><div class="k">Occupied</div></div>
<div class="metric"><div class="v ok-v">{{capacity.free}}</div><div class="k">Free</div></div>
<div class="metric"><div class="v warn-v">{{capacity.reserved}}</div><div class="k">Reserved</div></div>
<div class="metric"><div class="v">{{stats.total_arrivals}}</div><div class="k">Arrivals</div></div>
<div class="metric"><div class="v">{{stats.total_departed}}</div><div class="k">Departed</div></div>
<div class="metric"><div class="v ok-v">{{'%.2f'|format(stats.total_revenue)}}</div><div class="k">Revenue</div></div>
<div class="metric"><div class="v danger-v">{{stats.total_penalties}}</div><div class="k">Penalties</div></div>
</div>

<div class="card alert-card">
<div class="alert-title">
<h2 style="margin:0;color:#fca5a5">🚨 Critical Alerts</h2>
<span class="mini">{{alerts|length}} recent alert(s)</span>
</div>
<div class="alert-scroll">
{% if alerts %}
{% for a in alerts %}
<div class="alert {{'high' if a.severity=='HIGH' else ''}}">
<div><span class="sev">{{a.severity}}</span><div class="mini">{{a.alert_type}}</div></div>
<div><span class="plate">{{a.plate if a.plate else 'PLATE PENDING'}}</span><div class="mini">plate</div></div>
<div><b>{{a.display_entry}}</b><div class="mini">entered</div></div>
<div><b>{{a.display_exit_arrival}}</b><div class="mini">at exit</div></div>
<div><b>{{a.display_stuck}}</b><div class="mini">stuck</div></div>
<div><b>{{a.display_stay}}</b><div class="mini">total stay</div></div>
<div class="reason" title="{{a.reason}}">{{a.reason}}</div>
<div class="optional"><b>{{'%.2f'|format(a.fine_amount or 0)}}</b><div class="mini">fine</div></div>
</div>
{% endfor %}
{% else %}
<div class="empty">No penalties, payment failures or unpaid escapes detected.</div>
{% endif %}
</div>
</div>

<div class="grid" style="margin-top:10px">
<div class="card">
<h2>Live Vehicles</h2>
<div class="scroll">
<table>
<tr><th>Plate</th><th>Status</th><th>Spot</th><th>Entered</th><th>Exited</th><th>Total Stay</th><th>Billable</th><th>Charge</th><th>Payment</th><th>Action</th></tr>
{% for c in cars %}
<tr>
<td><b>{{c.plate}}</b></td>
<td>
<span class="status {{'status-bad' if c.status in ['PAYMENT_HOLD','ESCAPED_UNPAID','ROUTE_HOLD','ENTRY_ABORTED'] else ('status-paid' if c.status in ['PAID','LEFT','PARKED'] else 'status-warn')}}">
{{c.status}}
</span>
</td>
<td>{{c.actual_spot or c.assigned_spot or '-'}}</td>
<td>{{c.display_entry}}</td>
<td>{{c.display_exit}}</td>
<td><b>{{c.display_stay}}</b></td>
<td>{{c.billable_minutes or 0}}m</td>
<td>{{'%.2f'|format(c.total_charge or c.expected_amount or 0)}}</td>
<td>{{c.payment_status}}</td>
<td>
{% if c.status == 'PARKED' %}<form method="post" action="/car/{{c.plate}}/exit"><button>Exit</button></form>{% endif %}
{% if c.status == 'PAYMENT_HOLD' or c.payment_status in ['INVALID','CHARGE_ERROR'] %}<form method="post" action="/car/{{c.plate}}/retry-payment"><button class="danger">Retry Pay</button></form>{% endif %}
{% if c.status == 'ROUTE_HOLD' %}<form method="post" action="/car/{{c.plate}}/reassign"><button class="good">Reassign</button></form>{% endif %}
</td>
</tr>
{% endfor %}
</table>
</div>
</div>

<div class="card">
<h2>Gate Control</h2>
{% for g in gate_rows %}
<div class="gate">
<div><b>{{g.name}}</b><div class="mini">{{g.zone}} · {{g.state}} · health {{g.health}}% · override <b>{{g.override}}</b> {% if g.broken %}· BROKEN{% endif %}{% if g.maintenance %} · MAINTENANCE{% endif %}</div></div>
<div>
<form method="post" action="/gate/{{g.name}}/open"><button>Open</button></form>
<form method="post" action="/gate/{{g.name}}/close"><button class="gray">Close</button></form>
<form method="post" action="/gate/{{g.name}}/auto"><button class="gray">Auto</button></form>
{% if g.broken and not g.maintenance %}<form method="post" action="/component/gate/{{g.name}}/repair"><button class="danger">Repair</button></form>{% endif %}
</div>
</div>
{% endfor %}
<div class="mini" style="margin-top:7px">API-derived perimeter gates: {{perimeter_gates|join(' · ')}} · Exit queue: {{exit_waiting}}</div>

<h2 style="margin-top:13px">Zones</h2>
<div class="zones">
{% for z in zone_rows %}
<div class="zone">
<b>{{z.name}}</b>
<div class="mini">{{z.occupied}} occupied · {{z.free}} free · {{z.reserved}} reserved</div>
<div class="mini">CO {{z.co if z.co is not none else '-'}} · risk <b>{{z.risk}}</b></div>
<div class="mini">Fans {{z.fans_on}}/{{z.fans_total}} on · Lights {{z.lights_on}}/{{z.lights_total}} on</div>
<div class="bar"><i style="width:{{z.percent}}%"></i></div>
</div>
{% endfor %}
</div>
</div>
</div>

<div class="two">
<div class="card">
<h2>Parking Map</h2>
<div class="spots">
{% for s in spot_rows %}
<div class="spot {{'busy' if s.occupied else ('res' if s.reserved else '')}}" title="{{s.name}} · {{s.zone}} · health {{s.health}}%">
{{s.name}}
</div>
{% endfor %}
</div>
</div>

<div class="card">
<h2>Quick Search</h2>
<form class="toolbar" method="get" action="/search">
<input name="plate" placeholder="Plate number">
<button>Search Logs</button>
</form>
<div class="mini" style="margin-top:8px">Search car history, decisions and webhook events by plate/time.</div>

{% if is_admin %}
<div class="toolbar" style="margin-top:10px">
<form method="post" action="/sync"><button class="gray">Sync Simulator</button></form>
<form method="post" action="/admin/test-webhook"><button>Test Webhook</button></form>
<form method="post" action="/admin/reconcile-alerts"><button class="gray">Resolve Alert Plates</button></form>
</div>
{% endif %}
</div>
</div>

<details data-panel="level2-live">
<summary>Level 2 live infrastructure — API/Webhook state</summary>
<div class="mini" style="margin:8px 0">
Source of truth: simulator API + webhook cache · Last discovery sync: {{last_sync or '-'}} · Simulator time: {{simulator_time or '-'}}.
No dashboard polling of discovery endpoints.
</div>
<div class="two">
<div>
<h2>Exhaust Fans</h2>
<div class="compact-scroll">
<table><tr><th>Fan</th><th>Zone</th><th>State</th><th>Mode</th><th>Health</th><th>Control</th></tr>
{% for f in fan_rows %}
<tr>
<td><b>{{f.name}}</b></td><td>{{f.zone}}</td>
<td>{{'ON' if f.on else 'OFF'}}{% if f.broken %} · BROKEN{% endif %}{% if f.maintenance %} · MAINT{% endif %}</td>
<td>{{f.override}}</td><td>{{f.health}}%</td>
<td>
<form method="post" action="/fan/{{f.name}}/on"><button>On</button></form>
<form method="post" action="/fan/{{f.name}}/off"><button class="gray">Off</button></form>
<form method="post" action="/fan/{{f.name}}/auto"><button class="good">Auto</button></form>
{% if f.broken and not f.maintenance %}<form method="post" action="/component/fan/{{f.name}}/repair"><button class="danger">Repair</button></form>{% endif %}
</td>
</tr>
{% else %}<tr><td colspan="6">No fans returned by API.</td></tr>{% endfor %}
</table>
</div>
</div>
<div>
<h2>Maintenance Alarms</h2>
<div class="compact-scroll">
<table><tr><th>Component</th><th>Problem</th></tr>
{% for a in alarm_rows %}<tr><td><b>{{a.name}}</b></td><td>{{a.problem or 'Require Maintenance'}}</td></tr>
{% else %}<tr><td colspan="2">No active alarms returned/received.</td></tr>{% endfor %}
</table>
</div>
</div>
</div>

<h2 style="margin-top:12px">Parking Spot Maintenance</h2>
<div class="compact-scroll">
<table><tr><th>Spot</th><th>Zone</th><th>Type</th><th>Health</th><th>Status</th><th>Action</th></tr>
{% for s in spot_rows if s.broken or s.maintenance %}
<tr><td><b>{{s.name}}</b></td><td>{{s.zone}}</td><td>{{s.type}}</td><td>{{s.health}}%</td><td>{{'BROKEN' if s.broken else 'MAINTENANCE'}}</td>
<td>{% if s.broken and not s.maintenance %}<form method="post" action="/component/spot/{{s.name}}/repair"><button class="danger">Repair</button></form>{% else %}-{% endif %}</td></tr>
{% else %}<tr><td colspan="6">No broken/maintenance parking spots in live API cache.</td></tr>{% endfor %}
</table>
</div>

<h2 style="margin-top:12px">Lights</h2>
<div class="compact-scroll">
<table><tr><th>Light</th><th>Group</th><th>Zone</th><th>State</th><th>Mode</th><th>Control</th></tr>
{% for l in light_rows %}
<tr>
<td><b>{{l.name}}</b></td><td>{{l.group}}</td><td>{{l.zone}}</td><td>{{'ON' if l.on else 'OFF'}}</td><td>{{l.override}}</td>
<td>
<form method="post" action="/light/{{l.name}}/on"><button>On</button></form>
<form method="post" action="/light/{{l.name}}/off"><button class="gray">Off</button></form>
<form method="post" action="/light/{{l.name}}/auto"><button class="good">Auto</button></form>
</td>
</tr>
{% else %}<tr><td colspan="6">No lights returned by API.</td></tr>{% endfor %}
</table>
</div>
</details>

{% if is_admin %}
<details data-panel="admin-tools">
<summary>Admin tools, analytics and judge demo</summary>
<div class="two">
<div>
<h2>Judge Demo</h2>
{% if demo.enabled %}
<form method="post" action="/admin/demo/full"><button>Full Lot</button></form>
<form method="post" action="/admin/demo/fraud"><button>Payment Fraud</button></form>
<form method="post" action="/admin/demo/broken_gate"><button>Broken Gate</button></form>
<form method="post" action="/admin/demo/live_full"><button class="danger">LIVE Full Next Car</button></form>
<form method="post" action="/admin/demo/clear"><button class="gray">Clear</button></form>
{% else %}
<form method="post" action="/admin/demo/toggle"><button>Enable Judge Demo</button></form>
{% endif %}
</div>
<div>
<h2>Top Spots</h2>
<table><tr><th>Spot</th><th>Cars</th><th>Revenue</th><th>Avg</th></tr>
{% for s in spot_analytics %}<tr><td>{{s.spot}}</td><td>{{s.cars_hosted}}</td><td>{{'%.2f'|format(s.total_revenue)}}</td><td>{{s.avg_min}}m</td></tr>{% endfor %}
</table>
</div>
</div>
</details>

<details data-panel="decision-audit">
<summary>Recent decision audit</summary>
<div class="compact-scroll">
<table><tr><th>Time</th><th>Plate</th><th>Action</th><th>Detail / Why</th></tr>
{% for d in decisions %}
<tr><td>{{d.created_at[11:19] if d.created_at else '-'}}</td><td>{{d.plate or '-'}}</td><td>{{d.action}}</td><td>{{d.detail}} {% if d.reasoning %}<span class="mini">— {{d.reasoning}}</span>{% endif %}</td></tr>
{% endfor %}
</table>
</div>
</details>
{% endif %}

</div>
<script>
(function(){
  const storageKey = "parkmind-dashboard-panels";
  let saved = {};
  try { saved = JSON.parse(sessionStorage.getItem(storageKey) || "{}"); } catch(e) {}

  const panels = document.querySelectorAll("details[data-panel]");
  panels.forEach((panel) => {
    const key = panel.dataset.panel;
    if (Object.prototype.hasOwnProperty.call(saved, key)) {
      panel.open = !!saved[key];
    }
    panel.addEventListener("toggle", () => {
      saved[key] = panel.open;
      sessionStorage.setItem(storageKey, JSON.stringify(saved));
    });
  });

  // Keep the dashboard live without losing the user's open/closed panels.
  setTimeout(() => {
    panels.forEach((panel) => {
      saved[panel.dataset.panel] = panel.open;
    });
    sessionStorage.setItem(storageKey, JSON.stringify(saved));
    sessionStorage.setItem("parkmind-scroll-y", String(window.scrollY || 0));
    window.location.reload();
  }, 3000);

  const y = parseInt(sessionStorage.getItem("parkmind-scroll-y") || "0", 10);
  if (y > 0) window.scrollTo(0, y);
})();
</script>
</body>
</html>
"""



SEARCH_HTML = """
<!doctype html>
<html><head><title>PARKMIND — Search</title>
<style>
body{font-family:'Segoe UI',Arial;margin:0;background:#0f172a;color:#e5e7eb}
.wrap{padding:22px}.card{background:#1e293b;border:1px solid #334155;border-radius:14px;padding:18px;margin-bottom:14px}
input,button{padding:9px;border-radius:8px;border:1px solid #334155;background:#0f172a;color:#fff}
button{background:#0ea5e9;border:0;cursor:pointer}a{color:#22d3ee}
table{width:100%;border-collapse:collapse;font-size:12px}th,td{padding:7px;border-bottom:1px solid #334155;text-align:left}
th{color:#94a3b8}.muted{color:#94a3b8;font-size:12px}
</style></head><body><div class="wrap">
<p><a href="/">← Dashboard</a></p>
<div class="card"><h2>Search Parking Audit Log</h2>
<form method="get" action="/search">
<input name="plate" value="{{plate}}" placeholder="Plate / partial plate">
<label>From <input type="datetime-local" name="from" value="{{from_raw}}"></label>
<label>To <input type="datetime-local" name="to" value="{{to_raw}}"></label>
<button>Search</button>
</form>
<p class="muted">Searches cars, decisions and raw webhook events.</p>
</div>

<div class="card"><h2>Cars ({{cars|length}})</h2><table>
<tr><th>Plate</th><th>Status</th><th>Assigned</th><th>Actual</th><th>Entry</th><th>Departure</th><th>Paid</th></tr>
{% for c in cars %}
<tr><td>{{c.plate}}</td><td>{{c.status}}</td><td>{{c.assigned_spot or '-'}}</td><td>{{c.actual_spot or '-'}}</td><td>{{c.entry_time or '-'}}</td><td>{{c.departure_time or '-'}}</td><td>{{c.actual_paid or 0}}</td></tr>
{% endfor %}
</table></div>

<div class="card"><h2>Decisions ({{decisions|length}})</h2><table>
<tr><th>Time</th><th>Plate</th><th>Action</th><th>Detail</th><th>WHY</th></tr>
{% for d in decisions %}
<tr><td>{{d.created_at}}</td><td>{{d.plate or '-'}}</td><td>{{d.action}}</td><td>{{d.detail}}</td><td>{{d.reasoning}}</td></tr>
{% endfor %}
</table></div>

<div class="card"><h2>Webhook Events ({{events|length}})</h2><table>
<tr><th>Server Time</th><th>Class</th><th>Sequence</th><th>Payload</th></tr>
{% for e in events %}
<tr><td>{{e.server_time}}</td><td>{{e.event_class}}</td><td>{{e.sequence_id}}</td><td class="muted">{{e.payload}}</td></tr>
{% endfor %}
</table></div>
</div></body></html>
"""


def require_login():
    return bool(session.get("user"))


def require_admin():
    return require_login() and session.get("role") == "Admin"


@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        username = (request.form.get("username", "") or "").strip()[:80]
        password = (request.form.get("password", "") or "")[:256]
        ip = _client_ip()

        locked, remaining = _login_lock_status(username, ip)
        if locked:
            role = USERS.get(username, {}).get("role", "UNKNOWN")
            record_login_attempt(username, False, role)
            log_audit(
                "LOGIN_BLOCKED_LOCKOUT", username,
                f"ip={ip}; remaining_seconds={remaining}", "DENIED",
                actor=username or "UNKNOWN", role=role,
            )
            minutes_left = max(1, math.ceil(remaining / 60.0))
            error = f"Too many failed attempts. Try again in about {minutes_left} minute(s)."
            return render_template_string(LOGIN_HTML, error=error), 429

        user = _authenticate_user(username, password)
        if user:
            _reset_login_security(username, ip)
            record_login_attempt(username, True, user["role"])
            log_audit(
                "LOGIN", username, f"role={user['role']}; ip={ip}", "SUCCESS",
                actor=username, role=user["role"],
            )

            # Clearing first prevents session fixation; permanent=True enables
            # Flask's configured 30-minute rolling session lifetime.
            session.clear()
            session.permanent = True
            session["user"] = username
            session["role"] = user["role"]
            session["show_login_notice"] = True

            if user["role"] == "Maintenance":
                items = recent_login_attempts(3)
                summary = " | ".join(
                    f"{'SUCCESS' if x['success'] else 'FAILED'} {x['username']} {x['attempted_at']}"
                    for x in items
                )
                flash("Login security · last 3: " + summary)
                return redirect(url_for("maintenance.dashboard"))

            return redirect(url_for("dashboard"))

        failures, locked_until = _register_failed_auth(username, ip)
        role = USERS.get(username, {}).get("role", "UNKNOWN")
        record_login_attempt(username, False, role)
        log_audit(
            "LOGIN", username,
            f"Invalid credentials; ip={ip}; failures={failures}/{LOGIN_MAX_ATTEMPTS}",
            "FAILED", actor=username or "UNKNOWN", role=role,
        )

        if locked_until:
            log_audit(
                "LOGIN_LOCKOUT", username,
                f"ip={ip}; locked_until={locked_until}", "LOCKED",
                actor=username or "UNKNOWN", role=role,
            )
            error = (
                f"Too many failed attempts. Login locked for {LOGIN_LOCK_MINUTES} minute(s)."
            )
        else:
            remaining_attempts = max(0, LOGIN_MAX_ATTEMPTS - failures)
            error = f"Invalid username or password. {remaining_attempts} attempt(s) remaining."

    return render_template_string(LOGIN_HTML, error=error)


@app.route("/operator/login", methods=["GET", "POST"])
def operator_login():
    error = None
    if request.method == "POST":
        username = "operator"
        password = (request.form.get("password", "") or "")[:256]
        ip = _client_ip()

        locked, remaining = _login_lock_status(username, ip)
        if locked:
            record_login_attempt(username, False, "Operator")
            log_audit(
                "LOGIN_BLOCKED_LOCKOUT", username,
                f"ip={ip}; remaining_seconds={remaining}", "DENIED",
                actor=username, role="Operator",
            )
            minutes_left = max(1, math.ceil(remaining / 60.0))
            error = f"Too many failed attempts. Try again in about {minutes_left} minute(s)."
            return render_template_string(OPERATOR_LOGIN_HTML, error=error), 429

        user = _authenticate_user(username, password)
        if user and user.get("role") == "Operator":
            _reset_login_security(username, ip)
            record_login_attempt(username, True, "Operator")
            log_audit(
                "LOGIN", username, f"role=Operator; ip={ip}", "SUCCESS",
                actor=username, role="Operator",
            )
            session.clear()
            session.permanent = True
            session["user"] = username
            session["role"] = "Operator"
            session["show_login_notice"] = True
            return redirect(url_for("dashboard"))

        failures, locked_until = _register_failed_auth(username, ip)
        record_login_attempt(username, False, "Operator")
        log_audit(
            "LOGIN", username,
            f"Invalid operator credentials; ip={ip}; failures={failures}/{LOGIN_MAX_ATTEMPTS}",
            "FAILED", actor=username, role="Operator",
        )

        if locked_until:
            log_audit(
                "LOGIN_LOCKOUT", username,
                f"ip={ip}; locked_until={locked_until}", "LOCKED",
                actor=username, role="Operator",
            )
            error = (
                f"Too many failed attempts. Login locked for {LOGIN_LOCK_MINUTES} minute(s)."
            )
        else:
            remaining_attempts = max(0, LOGIN_MAX_ATTEMPTS - failures)
            error = f"Invalid operator password. {remaining_attempts} attempt(s) remaining."

    return render_template_string(OPERATOR_LOGIN_HTML, error=error)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/")
def dashboard():
    if not require_login():
        return redirect(url_for("login"))

    if session.get("role") == "Maintenance":
        return redirect(url_for("maintenance.dashboard"))

    show_login_notice = bool(session.pop("show_login_notice", False))
    recent_logins = recent_login_attempts(3) if show_login_notice else []

    with state_lock:
        spot_rows = []
        for name in sorted(spots.keys(), key=natural_spot_key):
            s = spots[name]
            spot_rows.append({
                "name": name,
                "zone": s.get("zoneParent", "?"),
                "occupied": bool(s.get("occupied")),
                "reserved": name in reserved_spots,
                "busy": bool(s.get("occupied")) or name in reserved_spots,
                "type": s.get("parkingForCarType", "Any"),
                "broken": bool(s.get("broken", False) or name in alarms),
                "maintenance": bool(s.get("isUnderMaintenance", False)),
                "health": component_health_score(name, "spot"),
                "uses": s.get("usage_count", 0),
            })
        gate_rows = [{
            "name": n,
            "zone": g.get("zoneParent", "") or "PERIMETER",
            "state": g.get("state", "?"),
            "broken": bool(g.get("broken", False) or n in alarms),
            "maintenance": bool(g.get("isUnderMaintenance", False)),
            "health": component_health_score(n, "gate"),
            "override": ("OPEN" if manual_overrides["gate"].get(n) is True else
                         "CLOSED" if manual_overrides["gate"].get(n) is False else "AUTO"),
        } for n, g in gates.items()]

        fan_rows = [{
            "name": n,
            "zone": f.get("zoneParent", "") or "UNASSIGNED",
            "on": bool(f.get("isOn")),
            "broken": bool(f.get("broken", False) or n in alarms),
            "maintenance": bool(f.get("isUnderMaintenance", False)),
            "health": component_health_score(n, "fan"),
            "override": ("ON" if manual_overrides["fan"].get(n) is True else
                         "OFF" if manual_overrides["fan"].get(n) is False else "AUTO"),
        } for n, f in sorted(fans.items())]

        light_rows = [{
            "name": n,
            "group": l.get("group", "") or "-",
            "zone": l.get("zoneParent", "") or "UNASSIGNED",
            "on": bool(l.get("isOn")),
            "health": component_health_score(n, "light"),
            "override": ("ON" if manual_overrides["light"].get(n) is True else
                         "OFF" if manual_overrides["light"].get(n) is False else "AUTO"),
        } for n, l in sorted(lights.items())]

        alarm_rows = [dict(v) for _, v in sorted(alarms.items())]
        snapshot_stats = dict(stats)

        # Requirement: occupied/free parking spots by zone.
        zone_acc = {}
        for row in spot_rows:
            z = row["zone"] or "UNASSIGNED"
            item = zone_acc.setdefault(z, {"name": z, "total": 0, "occupied": 0, "reserved": 0})
            item["total"] += 1
            item["occupied"] += 1 if row["occupied"] else 0
            item["reserved"] += 1 if row["reserved"] and not row["occupied"] else 0

        zone_rows = []
        all_zone_names = sorted(set(zone_acc) | set(zones))
        for z in all_zone_names:
            item = dict(zone_acc.get(z, {"name": z, "total": 0, "occupied": 0, "reserved": 0}))
            item["free"] = max(0, item["total"] - item["occupied"] - item["reserved"])
            item["percent"] = round((item["occupied"] / item["total"]) * 100) if item["total"] else 0
            zapi = zones.get(z, {})
            item["co"] = zapi.get("gasCarbonMonoxideLevel")
            item["risk"] = zapi.get("risk", "UNKNOWN")
            item["fans_on"] = sum(1 for f in fans.values() if str(f.get("zoneParent") or "") == z and f.get("isOn"))
            item["fans_total"] = sum(1 for f in fans.values() if str(f.get("zoneParent") or "") == z)
            item["lights_on"] = sum(1 for l in lights.values() if str(l.get("zoneParent") or "") == z and l.get("isOn"))
            item["lights_total"] = sum(1 for l in lights.values() if str(l.get("zoneParent") or "") == z)
            zone_rows.append(item)

    # Repair any older alerts whose penalty webhook did not provide a plate.
    reconcile_unresolved_alerts()

    conn = db()
    cars = [dict(r) for r in conn.execute(
        """SELECT * FROM cars
           ORDER BY
             CASE
               WHEN status IN ('ESCAPED_UNPAID','PAYMENT_HOLD','ROUTE_HOLD','ENTRY_ABORTED') THEN 0
               WHEN status IN ('WAITING','ASSIGNED','REROUTING','TO_EXIT','AT_EXIT','PAYMENT_PENDING','PAID','PARKED') THEN 1
               ELSE 2
             END,
             COALESCE(entry_time,'') DESC
           LIMIT 24"""
    ).fetchall()]

    # Exact, user-facing time fields.
    now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for c in cars:
        entry = c.get("entry_time")
        departure = c.get("departure_time")

        c["display_entry"] = entry[11:19] if entry else "-"
        c["display_exit"] = departure[11:19] if departure else "-"

        # Finished trip: ENTRY -> EXIT CarOut.
        # Active trip: ENTRY -> current time.
        stay_seconds = int(c.get("total_stay_seconds") or 0)
        if not stay_seconds and entry:
            stay_seconds = seconds_between(entry, departure or now_text)

        c["display_stay"] = format_duration(stay_seconds)

    alerts = [dict(r) for r in conn.execute(
        """SELECT * FROM alerts
           ORDER BY id DESC
           LIMIT 12"""
    ).fetchall()]

    for a in alerts:
        stay_seconds = int(a.get("stay_seconds") or 0)
        if not stay_seconds and a.get("entry_time") and a.get("event_time"):
            stay_seconds = seconds_between(a.get("entry_time"), a.get("event_time"))

        stuck_seconds = int(a.get("stuck_seconds") or 0)
        if (
            not stuck_seconds
            and a.get("exit_arrival_time")
            and a.get("alert_type") == "CAR STUCK AT EXIT"
        ):
            stuck_seconds = seconds_between(a.get("exit_arrival_time"), now_text)

        a["display_entry"] = (
            str(a.get("entry_time"))[11:19]
            if a.get("entry_time") else "-"
        )
        a["display_exit_arrival"] = (
            str(a.get("exit_arrival_time"))[11:19]
            if a.get("exit_arrival_time") else "-"
        )
        a["display_stuck"] = (
            format_duration(stuck_seconds)
            if stuck_seconds > 0 else "-"
        )
        a["display_stay"] = format_duration(stay_seconds)
    decisions = [dict(r) for r in conn.execute(
        "SELECT * FROM decisions ORDER BY id DESC LIMIT 30"
    ).fetchall()]
    spot_analytics = [dict(r) for r in conn.execute(
        """SELECT spot, cars_hosted, total_revenue,
                  CASE WHEN cars_hosted>0 THEN total_minutes/cars_hosted ELSE 0 END as avg_min
           FROM spot_stats ORDER BY total_revenue DESC LIMIT 8"""
    ).fetchall()]

    # Arrival sparkline: last 10 clock-minutes including current minute.
    now_min = datetime.now().replace(second=0, microsecond=0)
    minute_points = [now_min - timedelta(minutes=i) for i in range(9, -1, -1)]
    arrival_bars = []
    window_start = minute_points[0].strftime("%Y-%m-%d %H:%M:%S")
    raw_entries = conn.execute(
        """SELECT entry_time
           FROM cars
           WHERE entry_time IS NOT NULL
             AND entry_time >= ?
           ORDER BY entry_time ASC""",
        (window_start,)
    ).fetchall()
    counts = Counter()
    for r in raw_entries:
        try:
            dt = datetime.strptime(r["entry_time"], "%Y-%m-%d %H:%M:%S").replace(second=0)
            counts[dt] += 1
        except Exception:
            pass
    max_count = max([counts[p] for p in minute_points] + [1])
    for p in minute_points:
        count = counts[p]
        arrival_bars.append({
            "label": p.strftime("%Y-%m-%d %H:%M"),
            "short": p.strftime(":%M") if p.minute % 2 == 0 else "",
            "count": count,
            "height": max(3, round((count / max_count) * 72))
        })

    conn.close()

    capacity = {
        "total": len(spot_rows),
        "occupied": sum(1 for s in spot_rows if s["occupied"]),
        "reserved": sum(1 for s in spot_rows if s["reserved"] and not s["occupied"]),
    }
    capacity["free"] = max(
        0, capacity["total"] - capacity["occupied"] - capacity["reserved"]
    )

    if require_admin():
        admin_ctx = _admin_dashboard_context(
            spot_rows, gate_rows, fan_rows, zone_rows, alerts, snapshot_stats
        )
        return render_template_string(
            ADMIN_DASH_HTML,
            user=session["user"], role=session["role"],
            spot_rows=spot_rows, gate_rows=gate_rows, cars=cars,
            alerts=alerts, capacity=capacity,
            decisions=decisions, spot_analytics=spot_analytics,
            stats=snapshot_stats,
            entry_gate=ENTRY_GATE, exit_gate=EXIT_GATE,
            perimeter_gates=perimeter_gates, zone_gates=dict(zone_gates),
            exit_waiting=len(physical_exit_queue),
            zone_rows=zone_rows, fan_rows=fan_rows, light_rows=light_rows, alarm_rows=alarm_rows,
            simulator_time=last_simulator_time, last_sync=last_sync_at,
            arrival_bars=arrival_bars, demo=demo_state,
            recent_logins=recent_logins,
            **admin_ctx
        )

    return render_template_string(
        DASH_HTML, user=session["user"], role=session["role"],
        spot_rows=spot_rows, gate_rows=gate_rows, cars=cars,
        alerts=alerts, capacity=capacity,
        decisions=decisions, spot_analytics=spot_analytics,
        stats=snapshot_stats,
        entry_gate=ENTRY_GATE, exit_gate=EXIT_GATE,
        perimeter_gates=perimeter_gates, zone_gates=dict(zone_gates),
        exit_waiting=len(physical_exit_queue),
        zone_rows=zone_rows, fan_rows=fan_rows, light_rows=light_rows, alarm_rows=alarm_rows,
        simulator_time=last_simulator_time, last_sync=last_sync_at,
        arrival_bars=arrival_bars,
        recent_logins=recent_logins,
        is_admin=False,
        demo=demo_state
    )


def _normalize_search_time(raw, end=False):
    raw = (raw or "").strip()
    if not raw:
        return None
    value = raw.replace("T", " ")
    if len(value) == 16:
        value += ":59" if end else ":00"
    return value


@app.route("/search")
def search_logs():
    if not require_login():
        return redirect(url_for("login"))

    plate = (request.args.get("plate") or "").strip()
    from_raw = (request.args.get("from") or "").strip()
    to_raw = (request.args.get("to") or "").strip()
    from_ts = _normalize_search_time(from_raw)
    to_ts = _normalize_search_time(to_raw, end=True)

    car_where, car_args = ["1=1"], []
    dec_where, dec_args = ["1=1"], []
    evt_where, evt_args = ["1=1"], []

    if plate:
        like = f"%{plate}%"
        car_where.append("plate LIKE ?")
        car_args.append(like)
        dec_where.append("plate LIKE ?")
        dec_args.append(like)
        evt_where.append("payload LIKE ?")
        evt_args.append(like)

    if from_ts:
        car_where.append("COALESCE(entry_time, departure_time, '') >= ?")
        car_args.append(from_ts)
        dec_where.append("created_at >= ?")
        dec_args.append(from_ts)
        evt_where.append("COALESCE(server_time,'') >= ?")
        evt_args.append(from_ts)

    if to_ts:
        car_where.append("COALESCE(entry_time, departure_time, '') <= ?")
        car_args.append(to_ts)
        dec_where.append("created_at <= ?")
        dec_args.append(to_ts)
        evt_where.append("COALESCE(server_time,'') <= ?")
        evt_args.append(to_ts)

    conn = db()
    cars = [dict(r) for r in conn.execute(
        f"SELECT * FROM cars WHERE {' AND '.join(car_where)} ORDER BY COALESCE(entry_time,'') DESC LIMIT 200",
        car_args
    ).fetchall()]
    decisions = [dict(r) for r in conn.execute(
        f"SELECT * FROM decisions WHERE {' AND '.join(dec_where)} ORDER BY id DESC LIMIT 300",
        dec_args
    ).fetchall()]
    events = [dict(r) for r in conn.execute(
        f"SELECT server_time,event_class,sequence_id,payload FROM events WHERE {' AND '.join(evt_where)} ORDER BY id DESC LIMIT 300",
        evt_args
    ).fetchall()]
    conn.close()

    return render_template_string(
        SEARCH_HTML,
        plate=plate,
        from_raw=from_raw,
        to_raw=to_raw,
        cars=cars,
        decisions=decisions,
        events=events
    )


@app.route("/sync", methods=["POST"])
def sync_route():
    if not require_admin():
        return ("Admin only", 403)
    safe_sync_state("manual")
    log_audit("SIMULATOR_SYNC", "topology", "Manual topology sync requested", "SUCCESS")
    return redirect(url_for("dashboard"))


@app.route("/gate/<name>/<action>", methods=["POST"])
def manual_gate(name, action):
    if not require_login():
        return redirect(url_for("login"))
    try:
        if name not in gates:
            return ("Unknown gate - not present in simulator discovery cache", 404)

        if action == "auto":
            with state_lock:
                manual_overrides["gate"].pop(name, None)
                manual_gate_closed.discard(name)
            log_decision(
                "", "MANUAL_GATE_AUTO", name,
                "Operator returned gate control to PARKMIND automation."
            )
            # Re-apply the normal idle policy immediately where possible.
            ensure_keep_open_gates()

        elif action == "open":
            # Persist OPEN before sending the command so concurrent watchdogs
            # cannot race in and close the gate a moment later.
            with state_lock:
                manual_overrides["gate"][name] = True
                manual_gate_closed.discard(name)
            if str(gates.get(name, {}).get("state") or "") not in ("Open", "Opening"):
                open_gate(name, manual=True)
            log_decision(
                "", "MANUAL_GATE_OPEN_HOLD", name,
                "Dashboard OPEN override enabled; automation is not allowed to close this gate until AUTO or CLOSE is selected."
            )

        elif action == "close":
            if name == ENTRY_GATE:
                log_decision(
                    "", "MANUAL_ENTRY_CLOSE_BLOCKED", name,
                    "Dashboard close rejected because this barrier is required for simulator arrival spawning."
                )
                log_audit("MANUAL_GATE", name, "action=close; spawn-protected entry gate", "DENIED")
                return redirect(url_for("dashboard"))
            else:
                with state_lock:
                    manual_overrides["gate"][name] = False
                    manual_gate_closed.add(name)
                if str(gates.get(name, {}).get("state") or "") not in ("Closed", "Closing"):
                    close_gate(name, force=True, manual=True)
                log_decision(
                    "", "MANUAL_GATE_CLOSED_HOLD", name,
                    "Dashboard CLOSE override enabled; automation is not allowed to reopen this gate until AUTO or OPEN is selected."
                )
        else:
            return ("Invalid gate action", 400)
        log_audit("MANUAL_GATE", name, f"action={action}", "SUCCESS")
    except Exception as e:
        log_decision("", "MANUAL_GATE_ERROR", f"{name}: {e}")
        log_audit("MANUAL_GATE", name, f"action={action}; error={e}", "FAILED")
    return redirect(url_for("dashboard"))


@app.route("/fan/<name>/<action>", methods=["POST"])
def manual_fan(name, action):
    if not require_login():
        return redirect(url_for("login"))
    try:
        if name not in fans:
            return ("Unknown fan - not present in simulator discovery cache", 404)
        if action == "auto":
            manual_overrides["fan"].pop(name, None)
            apply_co_policy(str(fans[name].get("zoneParent") or ""))
        elif action in ("on", "off"):
            desired = action == "on"
            manual_overrides["fan"][name] = desired
            set_fan(name, desired, "Operator manual override via dashboard")
        log_audit("MANUAL_FAN", name, f"action={action}", "SUCCESS")
    except Exception as e:
        log_decision("", "MANUAL_FAN_ERROR", f"{name}: {e}")
        log_audit("MANUAL_FAN", name, f"action={action}; error={e}", "FAILED")
    return redirect(url_for("dashboard"))


@app.route("/light/<name>/<action>", methods=["POST"])
def manual_light(name, action):
    if not require_login():
        return redirect(url_for("login"))
    try:
        if name not in lights:
            return ("Unknown light - not present in simulator discovery cache", 404)
        if action == "auto":
            manual_overrides["light"].pop(name, None)
            apply_light_policy()
        elif action in ("on", "off"):
            desired = action == "on"
            manual_overrides["light"][name] = desired
            set_light(name, desired, "Operator manual override via dashboard")
        log_audit("MANUAL_LIGHT", name, f"action={action}", "SUCCESS")
    except Exception as e:
        log_decision("", "MANUAL_LIGHT_ERROR", f"{name}: {e}")
        log_audit("MANUAL_LIGHT", name, f"action={action}; error={e}", "FAILED")
    return redirect(url_for("dashboard"))


@app.route("/light-group/<group>/<action>", methods=["POST"])
def manual_light_group(group, action):
    if not require_login():
        return redirect(url_for("login"))
    try:
        if action == "auto":
            manual_overrides["light_group"].pop(group, None)
            apply_light_policy()
        elif action in ("on", "off"):
            desired = action == "on"
            manual_overrides["light_group"][group] = desired
            set_light_group(group, desired, "Operator manual group override via dashboard")
        log_audit("MANUAL_LIGHT_GROUP", group, f"action={action}", "SUCCESS")
    except Exception as e:
        log_decision("", "MANUAL_LIGHT_GROUP_ERROR", f"{group}: {e}")
        log_audit("MANUAL_LIGHT_GROUP", group, f"action={action}; error={e}", "FAILED")
    return redirect(url_for("dashboard"))


@app.route("/component/<kind>/<name>/repair", methods=["POST"])
def manual_component_repair(kind, name):
    if not require_login():
        return redirect(url_for("login"))
    if session.get("role") not in ("Admin", "Maintenance"):
        log_audit("REPAIR_BLOCKED_RBAC", f"{kind}:{name}", "Operator is not authorized to repair components", "DENIED")
        return ("Maintenance/Admin only", 403)
    source = {"gate": gates, "fan": fans, "spot": spots}.get(kind)
    if source is None or name not in source:
        return ("Unknown/unsupported component from simulator discovery cache", 404)
    try:
        repair_component(name)
        log_audit("REPAIR_REQUEST", f"{kind}:{name}", "Repair command submitted", "SUCCESS")
    except Exception as e:
        log_decision("", "MANUAL_REPAIR_ERROR", f"{kind}:{name}: {e}")
        log_audit("REPAIR_REQUEST", f"{kind}:{name}", str(e), "FAILED")
    return redirect(url_for("dashboard"))


@app.route("/car/<plate>/exit", methods=["POST"])
def manual_exit(plate):
    if not require_login():
        return redirect(url_for("login"))
    threading.Thread(target=request_exit_lane, args=(plate,), daemon=True).start()
    log_audit("MANUAL_EXIT_REQUEST", plate, "Operator/Admin requested normal exit-lane workflow", "REQUESTED")
    return redirect(url_for("dashboard"))


@app.route("/car/<plate>/reassign", methods=["POST"])
def reassign_route_hold(plate):
    """Operator recovery for a vehicle isolated in ROUTE_HOLD."""
    if not require_login():
        return redirect(url_for("login"))

    car = get_car(plate)
    if not car:
        return ("Unknown car", 404)
    log_audit("MANUAL_REASSIGN_REQUEST", plate, f"status={car.get('status')}", "REQUESTED")

    if car.get("status") != "ROUTE_HOLD":
        log_decision(
            plate, "MANUAL_REASSIGN_BLOCKED",
            f"status={car.get('status')}",
            "Manual reassign is only available for ROUTE_HOLD vehicles."
        )
        return redirect(url_for("dashboard"))

    car_type = car.get("car_type") or "Normal"
    planned = int(car.get("planned_minutes") or 0)
    new_spot, reason = choose_spot(car_type, planned)

    if not new_spot:
        log_decision(
            plate, "MANUAL_REASSIGN_NO_SPACE",
            "No safe free spot is available yet.",
            "Vehicle remains in ROUTE_HOLD; operator can retry when capacity changes."
        )
        return redirect(url_for("dashboard"))

    with state_lock:
        reserved_spots[new_spot] = {
            "plate": plate,
            "reserved_at": time.time()
        }

    try:
        upsert_car(
            plate,
            assigned_spot=new_spot,
            status="REROUTING",
            decision=reason
        )
        send_car(plate, new_spot)
        append_journey(plate, f"operator_reassign->{new_spot}")
        log_decision(
            plate, "MANUAL_REASSIGN",
            f"ROUTE_HOLD -> {new_spot}",
            f"Operator recovery using safe smart selection. {reason}"
        )

    except Exception as e:
        with state_lock:
            res = reserved_spots.get(new_spot)
            if res and res.get("plate") == plate:
                reserved_spots.pop(new_spot, None)

        upsert_car(plate, status="ROUTE_HOLD")
        log_decision(
            plate, "MANUAL_REASSIGN_ERROR",
            str(e),
            "Reservation released; vehicle remains isolated in ROUTE_HOLD."
        )

    return redirect(url_for("dashboard"))


@app.route("/car/<plate>/retry-payment", methods=["POST"])
def retry_payment(plate):
    if not require_login():
        return redirect(url_for("login"))

    car = get_car(plate)
    if not car:
        return ("Unknown car", 404)
    log_audit("PAYMENT_RETRY_REQUEST", plate, f"status={car.get('status')} payment={car.get('payment_status')}", "REQUESTED")

    if car.get("status") not in ("AT_EXIT", "PAYMENT_PENDING", "PAYMENT_HOLD") and \
       car.get("payment_status") not in ("INVALID", "CHARGE_ERROR", "WAITING_TO_CHARGE"):
        log_decision(
            plate, "PAYMENT_RETRY_BLOCKED",
            f"status={car.get('status')} payment={car.get('payment_status')}",
            "Retry allowed only for a vehicle currently held at payment/exit."
        )
        return redirect(url_for("dashboard"))

    parking_cost = float(car.get("parking_cost") or 0)
    charging_cost = float(car.get("charging_cost") or 0)
    if parking_cost + charging_cost <= 0:
        # Fallback to stored expected total for older DB rows.
        parking_cost = float(car.get("expected_amount") or 0)
        charging_cost = 0.0

    try:
        upsert_car(plate, payment_status="REQUESTED", status="PAYMENT_PENDING")
        charge_car(plate, parking_cost, charging_cost)
        log_decision(
            plate, "PAYMENT_RETRY",
            f"parking={parking_cost}, charging={charging_cost}",
            "Operator requested a legitimate retry after insufficient/failed payment."
        )
    except Exception as e:
        upsert_car(plate, payment_status="CHARGE_ERROR", status="PAYMENT_HOLD")
        log_decision(plate, "PAYMENT_RETRY_ERROR", str(e))

    return redirect(url_for("dashboard"))


@app.route("/car/<plate>/note", methods=["POST"])
def car_note(plate):
    if not require_admin():
        return ("Admin only", 403)
    note = request.form.get("note", "").strip()
    if note:
        upsert_car(plate, operator_note=note)
        log_decision(plate, "OPERATOR_NOTE", note,
                     "Manual annotation — system will respect this.")
    return redirect(url_for("dashboard"))


@app.route("/admin/reconcile-alerts", methods=["POST"])
def admin_reconcile_alerts():
    if not require_admin():
        return ("Admin only", 403)
    reconcile_unresolved_alerts()
    return redirect(url_for("dashboard"))


@app.route("/admin/test-webhook", methods=["POST"])
def admin_test_webhook():
    if not require_admin():
        return ("Admin only", 403)
    try:
        sim_request("GET", "/test")
        log_decision(
            "", "TEST_WEBHOOK_TRIGGERED",
            "Called simulator /api/v1/test",
            "Live connectivity demo: simulator should POST test_webhook back to PARKMIND."
        )
    except Exception as e:
        log_decision("", "TEST_WEBHOOK_ERROR", str(e))
    return redirect(url_for("dashboard"))


@app.route("/admin/demo/toggle", methods=["POST"])
def demo_toggle():
    if not require_admin():
        return ("Admin only", 403)
    demo_state["enabled"] = not demo_state["enabled"]
    demo_state["last_scenario"] = None
    if not demo_state["enabled"]:
        demo_state["events"].clear()
    return redirect(url_for("dashboard"))


@app.route("/admin/demo/<scenario>", methods=["POST"])
def demo_scenario(scenario):
    if not require_admin():
        return ("Admin only", 403)
    if not demo_state["enabled"]:
        return ("Enable Judge Demo Mode first", 400)

    now = datetime.now().strftime("%H:%M:%S")
    messages = {
        "full": "SIMULATED FULL LOT: 0 safe spaces → incoming vehicle is denied an unsafe assignment.",
        "fraud": "SIMULATED INSUFFICIENT FUNDS: underpayment → PAYMENT_HOLD; gate stays closed; retry remains available.",
        "broken_gate": f"SIMULATED BROKEN GATE: {ENTRY_GATE} unsafe → automatic open is blocked and recovery is logged.",
        "live_full": "LIVE TEST ARMED: only the NEXT real arriving car will be treated as a full-lot arrival.",
        "clear": "Demo scenario list cleared."
    }
    if scenario not in messages:
        return ("Unknown demo scenario", 404)

    if scenario == "clear":
        demo_state["events"].clear()
        demo_state["last_scenario"] = None
        demo_state["live_full_next_arrival"] = False
    else:
        msg = messages[scenario]
        if scenario == "live_full":
            demo_state["live_full_next_arrival"] = True
        demo_state["last_scenario"] = msg
        demo_state["events"].append({"time": now, "scenario": scenario, "message": msg})
        demo_state["events"] = demo_state["events"][-10:]

    return redirect(url_for("dashboard"))


# =====================================================================
# LEVEL-2 COMPLIANCE PAGES / EXCEPTION RECOVERY
# =====================================================================
PENALTIES_HTML = """
<!doctype html><html><head><title>PARKMIND — Penalties</title>
<style>body{font-family:Segoe UI,Arial;background:#07111f;color:#e5e7eb;margin:0;padding:20px}a{color:#67e8f9}table{width:100%;border-collapse:collapse;background:#101b2d}th,td{padding:9px;border-bottom:1px solid #26364e;text-align:left}th{color:#94a3b8;font-size:11px}.bad{color:#f87171}.mini{font-size:10px;color:#94a3b8}</style></head><body>
<p><a href="/">← Dashboard</a></p><h1>Simulator Penalties</h1>
<div class="mini">Every simulator penalty is stored independently of the live dashboard.</div><br>
<table><tr><th>Time</th><th>Plate</th><th>Reason</th><th>Fine</th></tr>
{% for p in rows %}<tr><td>{{p.event_time}}</td><td>{{p.plate or '-'}}</td><td>{{p.reason or '-'}}</td><td class="bad">{{'%.2f'|format(p.fine_amount or 0)}}</td></tr>
{% else %}<tr><td colspan="4">No penalties recorded.</td></tr>{% endfor %}</table></body></html>
"""

DAILY_REPORT_HTML = """
<!doctype html><html><head><title>PARKMIND — Daily Report</title>
<style>body{font-family:Segoe UI,Arial;background:#07111f;color:#e5e7eb;margin:0;padding:20px}a{color:#67e8f9}.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin:16px 0}.c{background:#101b2d;border:1px solid #26364e;border-radius:10px;padding:12px}.v{font-size:22px;font-weight:800}.mini{font-size:10px;color:#94a3b8}table{width:100%;border-collapse:collapse;background:#101b2d;margin-top:12px}th,td{padding:8px;border-bottom:1px solid #26364e;text-align:left}input,button{padding:7px;border-radius:7px;border:1px solid #334155;background:#0c1727;color:#fff}</style></head><body>
<p><a href="/">← Dashboard</a></p><h1>Dynamic Daily Operations Report</h1>
<form><input type="date" name="date" value="{{date}}"><button>Load</button></form>
<div class="mini">All figures use stored PARKMIND/simulator event timestamps for {{date}}.</div>
<div class="cards">
<div class="c"><div class="v">{{summary.arrivals}}</div><div class="mini">Arrivals</div></div>
<div class="c"><div class="v">{{summary.departures}}</div><div class="mini">Departures</div></div>
<div class="c"><div class="v">{{'%.2f'|format(summary.revenue)}}</div><div class="mini">Accepted Revenue</div></div>
<div class="c"><div class="v">{{summary.penalties}}</div><div class="mini">Penalties</div></div>
<div class="c"><div class="v">{{summary.alerts}}</div><div class="mini">Important Alerts</div></div>
<div class="c"><div class="v">{{summary.repairs}}</div><div class="mini">Maintenance Jobs</div></div>
<div class="c"><div class="v">{{summary.failed_logins}}</div><div class="mini">Failed Logins</div></div>
</div>
<h2>Important Alerts</h2><table><tr><th>Time</th><th>Type</th><th>Target</th><th>Reason</th></tr>
{% for a in alerts %}<tr><td>{{a.event_time or a.created_at}}</td><td>{{a.alert_type}}</td><td>{{a.plate or '-'}}</td><td>{{a.reason}}</td></tr>
{% else %}<tr><td colspan="4">No alerts for this date.</td></tr>{% endfor %}</table>
<h2>Accepted Payments</h2><table><tr><th>Plate</th><th>Parking</th><th>Charging</th><th>Total</th></tr>
{% for p in payments %}<tr><td>{{p.plate}}</td><td>{{'%.2f'|format(p.parking_cost or 0)}}</td><td>{{'%.2f'|format(p.charging_cost or 0)}}</td><td>{{'%.2f'|format(p.actual_paid or 0)}}</td></tr>
{% else %}<tr><td colspan="4">No accepted payments for this date.</td></tr>{% endfor %}</table></body></html>
"""

AUDIT_HTML = """
<!doctype html><html><head><title>PARKMIND — Audit</title>
<style>body{font-family:Segoe UI,Arial;background:#07111f;color:#e5e7eb;margin:0;padding:20px}a{color:#67e8f9}table{width:100%;border-collapse:collapse;background:#101b2d}th,td{padding:8px;border-bottom:1px solid #26364e;text-align:left;font-size:11px}th{color:#94a3b8}.ok{color:#86efac}.bad{color:#f87171}</style></head><body>
<p><a href="/">← Dashboard</a> · <a href="/search">Search raw events/decisions</a></p><h1>Actor Audit Log</h1>
<table><tr><th>Time</th><th>Actor</th><th>Role</th><th>Action</th><th>Target</th><th>Detail</th><th>Result</th></tr>
{% for a in rows %}<tr><td>{{a.created_at}}</td><td>{{a.actor}}</td><td>{{a.role}}</td><td>{{a.action}}</td><td>{{a.target}}</td><td>{{a.detail}}</td><td class="{{'bad' if a.result in ('FAILED','DENIED','REJECTED') else 'ok'}}">{{a.result}}</td></tr>
{% else %}<tr><td colspan="7">No actor audit entries yet.</td></tr>{% endfor %}</table></body></html>
"""

@app.route("/audit")
def audit_page():
    if not require_admin():
        return ("Admin only", 403)
    conn = db()
    rows = [dict(r) for r in conn.execute("SELECT * FROM audit_log ORDER BY id DESC LIMIT 500").fetchall()]
    conn.close()
    return render_template_string(AUDIT_HTML, rows=rows)


@app.route("/penalties")
def penalties_page():
    if not require_login():
        return redirect(url_for("login"))
    conn = db()
    rows = []
    for raw in conn.execute("SELECT * FROM penalties ORDER BY id DESC LIMIT 300").fetchall():
        item = dict(raw)
        payload = {}
        try:
            payload = json.loads(item.get("payload") or "{}")
        except Exception:
            payload = {}
        item["plate"] = str(item.get("plate") or payload.get("CarPlateNumber") or payload.get("PlateNumber") or "")
        item["event_time"] = str(item.get("event_time") or payload.get("ServerDateTime") or item.get("detected_at") or "")
        rows.append(item)
    conn.close()
    return render_template_string(PENALTIES_HTML, rows=rows)


@app.route("/reports/daily")
def daily_report_page():
    if not require_admin():
        return ("Admin only", 403)
    selected = (request.args.get("date") or "").strip()
    if not selected:
        selected = str(last_simulator_time or "")[:10] or datetime.now().strftime("%Y-%m-%d")
    conn = db()
    time_expr = "COALESCE(departure_time, exit_arrival_time, parked_time, entry_time, '')"
    def one(sql, args=()):
        try:
            return conn.execute(sql, args).fetchone()[0] or 0
        except Exception:
            return 0
    summary = {
        "arrivals": int(one("SELECT COUNT(*) FROM cars WHERE substr(entry_time,1,10)=?", (selected,))),
        "departures": int(one("SELECT COUNT(*) FROM cars WHERE substr(departure_time,1,10)=?", (selected,))),
        "revenue": float(one(f"SELECT COALESCE(SUM(actual_paid),0) FROM cars WHERE payment_status='PAID' AND substr({time_expr},1,10)=?", (selected,))),
        "penalties": int(one("SELECT COUNT(*) FROM penalties WHERE substr(COALESCE(NULLIF(event_time,''),detected_at),1,10)=?", (selected,))),
        "alerts": int(one("SELECT COUNT(*) FROM alerts WHERE substr(COALESCE(event_time,created_at,''),1,10)=?", (selected,))),
        "failed_logins": int(one("SELECT COUNT(*) FROM login_attempts WHERE success=0 AND substr(attempted_at,1,10)=?", (selected,))),
        "repairs": 0,
    }
    try:
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        if "maintenance_dashboard_jobs" in tables:
            summary["repairs"] = int(one(
                "SELECT COUNT(*) FROM maintenance_dashboard_jobs WHERE substr(COALESCE(start_sim_time,started_at,''),1,10)=?",
                (selected,)
            ))
    except Exception:
        pass
    alerts = [dict(r) for r in conn.execute(
        "SELECT * FROM alerts WHERE substr(COALESCE(event_time,created_at,''),1,10)=? ORDER BY id DESC LIMIT 100",
        (selected,)
    ).fetchall()]
    payments = [dict(r) for r in conn.execute(
        f"SELECT plate,parking_cost,charging_cost,actual_paid FROM cars WHERE payment_status='PAID' AND substr({time_expr},1,10)=? ORDER BY {time_expr} DESC LIMIT 200",
        (selected,)
    ).fetchall()]
    conn.close()
    log_audit("DAILY_REPORT_VIEW", selected, "Dynamic daily operations/financial report opened", "SUCCESS")
    return render_template_string(DAILY_REPORT_HTML, date=selected, summary=summary, alerts=alerts, payments=payments)


@app.route("/admin/manual-car/recover", methods=["POST"])
def manual_car_recovery():
    """Audited exception flow for a real car that bypassed normal entry/spot sensing.

    This does not weaken ghost-payment checks. Instead an Admin must explicitly
    convert the physical exception into a tracked trip before normal payment/exit
    rules are allowed to operate.
    """
    if not require_admin():
        return ("Admin only", 403)

    plate = " ".join((request.form.get("plate") or "").upper().split())
    car_type = (request.form.get("car_type") or "Normal").strip()
    spot = (request.form.get("spot") or "").strip()
    start_exit = request.form.get("start_exit") == "1"
    try:
        estimated = max(1, min(1440, int(request.form.get("estimated_minutes") or 1)))
    except Exception:
        estimated = 1

    if not plate:
        return ("Plate is required", 400)
    if car_type not in ("Normal", "Electric", "Accessible"):
        return ("Unsupported car type", 400)

    existing = get_car(plate) or {}
    already_at_exit = existing.get("status") in ("AT_EXIT", "PAYMENT_PENDING", "PAYMENT_HOLD") or bool(existing.get("exit_arrival_time"))
    if not already_at_exit and (not spot or spot not in spots):
        return ("Select the physical parking bay for a vehicle that is not already at ExitSpot", 400)
    if spot and (spots.get(spot, {}).get("broken") or spots.get(spot, {}).get("isUnderMaintenance")):
        return ("Selected parking bay is unavailable/broken", 409)
    if spot and spots.get(spot, {}).get("occupied"):
        current = get_car(plate) or {}
        if current.get("actual_spot") != spot and current.get("assigned_spot") != spot:
            return ("Selected parking bay is already occupied", 409)
    if spot and spot in reserved_spots and reserved_spots.get(spot, {}).get("plate") != plate:
        return ("Selected parking bay is reserved for another vehicle", 409)

    now_dt = simulator_now() or datetime.now()
    observed_start = now_dt - timedelta(minutes=estimated)
    observed_text = observed_start.strftime("%Y-%m-%d %H:%M:%S")
    note = f"[MANUAL RECOVERY] Admin={session.get('user')} estimated={estimated}min spot={spot or 'EXIT'}"

    if already_at_exit:
        exit_time = existing.get("exit_arrival_time") or now_dt.strftime("%Y-%m-%d %H:%M:%S")
        upsert_car(
            plate, car_type=car_type, planned_minutes=estimated,
            entry_time=existing.get("entry_time") or observed_text,
            parked_time=existing.get("parked_time") or observed_text,
            operator_note=note,
            status="AT_EXIT"
        )
        minutes, parking_cost, charging_cost = calculate_charge(plate, exit_time)
        expected = parking_cost + charging_cost
        upsert_car(
            plate, exit_arrival_time=exit_time, billable_minutes=minutes,
            parking_cost=parking_cost, charging_cost=charging_cost,
            total_charge=expected, expected_amount=expected,
            payment_status="WAITING_TO_CHARGE", status="AT_EXIT"
        )
        with state_lock:
            charge_attempt_counts[plate] = 0
            charge_attempt_scheduled.discard(plate)
        schedule_charge_attempt(plate, parking_cost, charging_cost, trigger="manual recovery at ExitSpot")
        detail = f"Recovered existing ExitSpot vehicle; expected={expected:.2f}"
    else:
        with state_lock:
            spots[spot]["occupied"] = True
        upsert_car(
            plate, car_type=car_type, planned_minutes=estimated,
            assigned_spot=spot, actual_spot=spot,
            entry_time=observed_text, parked_time=observed_text,
            expected_amount=0, actual_paid=0, billable_minutes=0,
            parking_cost=0, charging_cost=0, total_charge=0,
            payment_status="NONE", status="PARKED", operator_note=note
        )
        append_journey(plate, f"manual_recovery@{spot}")
        detail = f"Registered physical exception at {spot}; estimated prior stay={estimated}min"
        if start_exit:
            threading.Thread(target=request_exit_lane, args=(plate,), daemon=True).start()
            detail += "; normal exit workflow started"

    conn = db()
    conn.execute(
        """INSERT INTO manual_recoveries(created_at,actor,plate,car_type,spot,estimated_minutes,recovery_state,detail)
           VALUES(?,?,?,?,?,?,?,?)""",
        (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), session.get("user"), plate, car_type, spot,
         estimated, "AT_EXIT" if already_at_exit else "REGISTERED", detail)
    )
    conn.commit()
    conn.close()
    log_decision(plate, "MANUAL_VEHICLE_RECOVERY", detail,
                 "Admin-authorized exception converted an untracked physical vehicle into a normal audited trip.")
    log_audit("MANUAL_VEHICLE_RECOVERY", plate, detail, "SUCCESS")
    return redirect(url_for("dashboard"))


# [INNOVATION #14] — CSV EXPORT
@app.route("/export/cars")
def export_cars():
    if not require_admin():
        return ("Admin only", 403)
    log_audit("FINANCIAL_EXPORT", "cars.csv", "Vehicle/payment CSV export requested", "SUCCESS")
    conn = db()
    rows = conn.execute("SELECT * FROM cars ORDER BY entry_time DESC").fetchall()
    conn.close()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "plate","car_type","planned_min","assigned_spot","actual_spot","route_mismatch_count",
        "entry_time","parked_time","exit_arrival_time","departure_time",
        "total_stay_minutes","total_stay_seconds","billable_minutes",
        "parking_cost","charging_cost","total_charge",
        "expected_amount","actual_paid","payment_status","status",
        "anomaly_flag","operator_note","journey"
    ])
    for r in rows:
        writer.writerow([
            r["plate"], r["car_type"], r["planned_minutes"],
            r["assigned_spot"], r["actual_spot"], r["route_mismatch_count"],
            r["entry_time"], r["parked_time"],
            r["exit_arrival_time"], r["departure_time"],
            r["total_stay_minutes"], r["total_stay_seconds"], r["billable_minutes"],
            r["parking_cost"], r["charging_cost"], r["total_charge"],
            r["expected_amount"], r["actual_paid"],
            r["payment_status"], r["status"], r["anomaly_flag"],
            r["operator_note"], r["journey"]
        ])
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment;filename=parkmind_cars.csv"}
    )




# =====================================================================
# EMBEDDED MAINTENANCE DASHBOARD
# =====================================================================
# The maintenance source is intentionally isolated inside this loader.
# Both original programs define helpers named db(), sim_login(), sim_request(),
# detected_count(), etc. Keeping maintenance in this closure prevents those names
# from overwriting the live parking controller while still producing ONE .py file.

def _build_maintenance_blueprint():
    from flask import (
        Blueprint,
        render_template_string,
        session,
        redirect,
        flash,
        request,
    )
    import sqlite3
    from pathlib import Path
    import os
    import json
    import requests
    from datetime import datetime
    from urllib.parse import quote

    # ============================================================
    # PARKMIND LEVEL 2
    # COMPACT MAINTENANCE + MANUAL CONTROL DASHBOARD
    # ============================================================

    maintenance_bp = Blueprint(
        "maintenance",
        __name__,
        url_prefix="/maintenance"
    )

    APP_DIR = Path(__file__).resolve().parent

    DB_PATH = Path(
        os.getenv(
            "PARKMIND_LEVEL2_DB",
            str(APP_DIR / "parkmind_level2.db")
        )
    )

    SIM_BASE = os.getenv(
        "PARKMIND_SIM_BASE",
        "http://127.0.0.1:9898/api/v1"
    )

    SIM_USER = os.getenv("PARKMIND_SIM_USER", "admin")
    SIM_PASSWORD = os.getenv("PARKMIND_SIM_PASSWORD", "admin")

    TOKEN = None


    # ============================================================
    # HTML
    # ============================================================

    HTML = r"""
    <!doctype html>
    <html>
    <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width,initial-scale=1">

    <title>PARKMIND · Maintenance</title>

    <style>

    *{
        box-sizing:border-box;
    }

    body{
        margin:0;
        background:#07111f;
        color:#e5e7eb;
        font-family:Inter,"Segoe UI",Arial,sans-serif;
    }

    header{
        position:sticky;
        top:0;
        z-index:50;
        min-height:68px;
        padding:10px 22px;
        background:#0b1729;
        border-bottom:1px solid #26364e;

        display:flex;
        align-items:center;
        justify-content:space-between;
        gap:15px;
    }

    .brand{
        font-size:20px;
        font-weight:900;
        color:#67e8f9;
    }

    .sub{
        color:#94a3b8;
        font-size:10px;
        margin-top:3px;
    }

    .header-right{
        display:flex;
        align-items:center;
        gap:8px;
        flex-wrap:wrap;
    }

    button{
        border:0;
        border-radius:8px;
        padding:8px 11px;
        font-size:10px;
        font-weight:800;
        cursor:pointer;
        color:white;
    }

    button:hover{
        filter:brightness(1.12);
    }

    button:disabled{
        opacity:.4;
        cursor:not-allowed;
    }

    .btn-blue{background:#0284c7}
    .btn-green{background:#047857}
    .btn-red{background:#b91c1c}
    .btn-yellow{background:#a16207}
    .btn-gray{background:#475569}
    .btn-dark{background:#1e293b}

    a{
        color:#67e8f9;
        text-decoration:none;
    }

    .page{
        max-width:1450px;
        margin:auto;
        padding:16px;
    }

    /* ============================================================
       STATUS
       ============================================================ */

    .statusbar{
        display:grid;
        grid-template-columns:repeat(4,1fr);
        gap:9px;
        margin-bottom:12px;
    }

    .status{
        background:#101b2d;
        border:1px solid #26364e;
        border-radius:12px;
        padding:12px;
    }

    .status .number{
        font-size:23px;
        font-weight:900;
    }

    .status .label{
        color:#94a3b8;
        font-size:9px;
        text-transform:uppercase;
        letter-spacing:.5px;
        margin-top:3px;
    }

    .good{
        color:#86efac;
    }

    .warn{
        color:#fbbf24;
    }

    .bad{
        color:#f87171;
    }

    .cyan{
        color:#67e8f9;
    }

    .info{
        color:#7dd3fc;
    }

    /* ============================================================
       BANNERS
       ============================================================ */

    .banner{
        padding:10px 12px;
        border-radius:10px;
        margin-bottom:10px;
        font-size:10px;
        line-height:1.5;
    }

    .banner.good{
        background:#0d2c23;
        border:1px solid #047857;
    }

    .banner.bad{
        background:#281116;
        border:1px solid #7f1d1d;
    }

    .banner.info{
        background:#082f49;
        border:1px solid #0e7490;
    }

    .flash{
        padding:9px 11px;
        border-radius:8px;
        margin-bottom:9px;
        background:#2a210d;
        border:1px solid #92400e;
        color:#fde68a;
        font-size:10px;
    }

    /* ============================================================
       MAIN SECTION
       ============================================================ */

    .section{
        background:#101b2d;
        border:1px solid #26364e;
        border-radius:14px;
        margin-bottom:11px;
        overflow:hidden;
    }

    .section-header{
        padding:13px 15px;
        display:flex;
        align-items:center;
        justify-content:space-between;
        cursor:pointer;
        background:#0f1a2c;
    }

    .section-header:hover{
        background:#132137;
    }

    .section-title{
        display:flex;
        align-items:center;
        gap:9px;
        font-size:12px;
        font-weight:900;
        text-transform:uppercase;
        letter-spacing:.5px;
    }

    .section-summary{
        font-size:10px;
        color:#94a3b8;
    }

    .chevron{
        font-size:14px;
        transition:.2s;
    }

    .section.open .chevron{
        transform:rotate(180deg);
    }

    .section-body{
        display:none;
        padding:12px;
        border-top:1px solid #26364e;
    }

    .section.open .section-body{
        display:block;
    }

    /* ============================================================
       COMPONENT GRID
       ============================================================ */

    .component-grid{
        display:grid;
        grid-template-columns:repeat(auto-fit,minmax(270px,1fr));
        gap:9px;
    }

    .component{
        background:#0b1525;
        border:1px solid #26364e;
        border-radius:11px;
        overflow:hidden;
    }

    .component-head{
        padding:11px;
        cursor:pointer;
    }

    .component-head:hover{
        background:#101d31;
    }

    .component-top{
        display:flex;
        align-items:center;
        justify-content:space-between;
        gap:8px;
    }

    .component-name{
        font-size:12px;
        font-weight:900;
    }

    .component-meta{
        color:#94a3b8;
        font-size:9px;
        margin-top:3px;
    }

    .component-state{
        margin-top:8px;
        display:flex;
        align-items:center;
        justify-content:space-between;
        gap:5px;
    }

    .component-details{
        display:none;
        padding:11px;
        border-top:1px solid #26364e;
        background:#091321;
    }

    .component.open .component-details{
        display:block;
    }

    .component-arrow{
        color:#64748b;
        transition:.2s;
    }

    .component.open .component-arrow{
        transform:rotate(180deg);
    }

    /* ============================================================
       BADGES
       ============================================================ */

    .badge{
        display:inline-block;
        padding:3px 7px;
        border-radius:999px;
        background:#1e293b;
        font-size:9px;
        font-weight:900;
    }

    .badge.good{
        background:#0f2f25;
        color:#86efac;
    }

    .badge.bad{
        background:#3a171b;
        color:#fca5a5;
    }

    .badge.warn{
        background:#3b2a10;
        color:#fde68a;
    }

    .badge.cyan{
        background:#082f49;
        color:#7dd3fc;
    }

    .badge.gray{
        color:#cbd5e1;
    }

    /* ============================================================
       DETAILS
       ============================================================ */

    .detail-grid{
        display:grid;
        grid-template-columns:1fr 1fr;
        gap:7px;
        margin-bottom:10px;
    }

    .detail{
        padding:8px;
        border:1px solid #26364e;
        border-radius:7px;
        background:#101b2d;
    }

    .detail-label{
        font-size:8px;
        color:#64748b;
        text-transform:uppercase;
    }

    .detail-value{
        margin-top:3px;
        font-size:10px;
        font-weight:800;
    }

    .reason{
        padding:8px;
        border-radius:7px;
        background:#111c2d;
        border:1px solid #26364e;
        font-size:9px;
        line-height:1.4;
        margin-bottom:8px;
    }

    /* ============================================================
       CONTROLS
       ============================================================ */

    .controls{
        display:flex;
        gap:6px;
        flex-wrap:wrap;
        margin-top:9px;
    }

    .control-label{
        font-size:8px;
        color:#64748b;
        text-transform:uppercase;
        margin-bottom:5px;
    }

    .control-group{
        margin-top:9px;
    }

    .manual-warning{
        padding:7px;
        border-radius:7px;
        font-size:9px;
        background:#281116;
        border:1px solid #7f1d1d;
        color:#fecaca;
    }

    .manual-info{
        padding:7px;
        border-radius:7px;
        font-size:9px;
        background:#082f49;
        border:1px solid #0e7490;
        color:#bae6fd;
    }

    /* ============================================================
       CO
       ============================================================ */

    .co-grid{
        display:grid;
        grid-template-columns:repeat(auto-fit,minmax(250px,1fr));
        gap:9px;
    }

    .zone{
        padding:11px;
        border-radius:10px;
        background:#0b1525;
        border:1px solid #26364e;
    }

    .zone.danger{
        border-color:#7f1d1d;
        background:#281116;
    }

    .zone-title{
        display:flex;
        justify-content:space-between;
        gap:10px;
    }

    .co-value{
        font-size:19px;
        font-weight:900;
        margin-top:8px;
    }

    /* ============================================================
       TABLES
       ============================================================ */

    .table-wrap{
        overflow:auto;
    }

    table{
        width:100%;
        border-collapse:collapse;
        font-size:9px;
    }

    th,td{
        padding:7px;
        border-bottom:1px solid #26364e;
        text-align:left;
    }

    th{
        color:#94a3b8;
        font-size:8px;
        text-transform:uppercase;
    }

    .empty{
        padding:20px;
        text-align:center;
        color:#64748b;
    }

    /* ============================================================
       MOBILE
       ============================================================ */

    @media(max-width:800px){

        header{
            position:relative;
            align-items:flex-start;
            flex-direction:column;
        }

        .statusbar{
            grid-template-columns:1fr 1fr;
        }

        .component-grid{
            grid-template-columns:1fr;
        }

    }

    </style>
    </head>

    <body>

    <header>

    <div>
        <div class="brand">PARKMIND · Maintenance</div>
        <div class="sub">
            Live maintenance · manual controls · safety interlocks
        </div>
    </div>

    <div class="header-right">

        <span class="sub">
            Simulator:
            <b class="{{'good' if online else 'bad'}}">
                {{'ONLINE' if online else 'OFFLINE'}}
            </b>
        </span>

        <form method="post" action="/maintenance/refresh">
            <button class="btn-gray">
                Refresh
            </button>
        </form>

        <a href="/logout">Logout</a>

    </div>

    </header>


    <div class="page">

    {% with messages = get_flashed_messages() %}
        {% for message in messages %}
            <div class="flash">{{message}}</div>
        {% endfor %}
    {% endwith %}


    <div class="banner {{'good' if online else 'bad'}}">

    {% if online %}

    <b>Simulator connected.</b>
    Last snapshot:
    {{last_refresh or 'not recorded'}}

    {% else %}

    <b>Simulator unavailable.</b>
    Start the Level 2 simulator and press Refresh.

    {% endif %}

    </div>


    <!-- ========================================================
         KPI
    ======================================================== -->

    <div class="statusbar">

        <div class="status">
            <div class="number bad">{{stats.broken}}</div>
            <div class="label">Broken</div>
        </div>

        <div class="status">
            <div class="number warn">{{stats.preventive}}</div>
            <div class="label">Maintenance Due</div>
        </div>

        <div class="status">
            <div class="number cyan">{{stats.locked}}</div>
            <div class="label">Locked</div>
        </div>

        <div class="status">
            <div class="number good">{{stats.fans_on}}</div>
            <div class="label">Fans Running</div>
        </div>

    </div>


    <!-- ========================================================
         FANS
    ======================================================== -->

    <div class="section open">

    <div class="section-header" onclick="toggleSection(this.parentElement)">

        <div>
            <div class="section-title">
                🌀 Exhaust Fans
            </div>

            <div class="section-summary">
                {{stats.fan_total}} fans ·
                {{stats.fans_on}} running ·
                {{stats.fans_broken}} broken
            </div>
        </div>

        <div class="chevron">▼</div>

    </div>


    <div class="section-body">

    <div class="component-grid">

    {% for c in fans %}

    <div class="component">

    <div class="component-head"
         onclick="toggleComponent(this.parentElement)">

    <div class="component-top">

    <div>
        <div class="component-name">{{c.name}}</div>
        <div class="component-meta">
            {{c.zone or 'Unknown zone'}}
        </div>
    </div>

    <div class="component-arrow">▼</div>

    </div>


    <div class="component-state">

    {% if c.broken %}

    <span class="badge bad">BROKEN</span>

    {% elif c.locked %}

    <span class="badge cyan">MAINTENANCE</span>

    {% elif c.is_on %}

    <span class="badge good">ON</span>

    {% else %}

    <span class="badge gray">OFF</span>

    {% endif %}


    {% if c.co_danger %}

    <span class="badge bad">CO RISK</span>

    {% endif %}

    </div>

    </div>


    <div class="component-details">

    <div class="detail-grid">

    <div class="detail">
        <div class="detail-label">Zone</div>
        <div class="detail-value">{{c.zone or '-'}}</div>
    </div>

    <div class="detail">
        <div class="detail-label">State</div>
        <div class="detail-value">{{'ON' if c.is_on else 'OFF'}}</div>
    </div>

    <div class="detail">
        <div class="detail-label">CO Risk</div>
        <div class="detail-value">{{c.risk or 'Unknown'}}</div>
    </div>

    <div class="detail">
        <div class="detail-label">Detected CO</div>
        <div class="detail-value">{{c.co_level if c.co_level is not none else '-'}}</div>
    </div>

    </div>


    {% if c.co_danger %}

    <div class="manual-warning">
        ⚠ CO safety mode active.
        Keep ventilation available.
        Manual OFF is blocked while this fan is required for active CO safety.
    </div>

    {% endif %}


    {% if c.locked %}

    <div class="manual-warning">
        🔒 This fan is under maintenance.
        Manual operation is disabled until maintenance is complete.
    </div>

    {% else %}

    <div class="control-group">

    <div class="control-label">
        Manual fan control
    </div>

    <div class="controls">

    <form method="post"
          action="/maintenance/manual/fan/{{c.name}}/on">

    <button
        class="btn-green"
        {% if c.is_on %}disabled{% endif %}>
        ▶ MANUAL ON
    </button>

    </form>


    <form method="post"
          action="/maintenance/manual/fan/{{c.name}}/off">

    <button
        class="btn-red"
        {% if not c.is_on or c.co_danger %}disabled{% endif %}>
        ■ MANUAL OFF
    </button>

    </form>

    </div>

    </div>

    {% endif %}


    {% if c.broken %}

    <div class="control-group">

    <div class="control-label">
        Maintenance
    </div>

    <form method="post"
          action="/maintenance/repair/fan/{{c.name}}">

    <button class="btn-yellow">
        🔧 Repair Fan
    </button>

    </form>

    </div>

    {% endif %}

    </div>

    </div>

    {% else %}

    <div class="empty">
        No exhaust fans returned by simulator.
    </div>

    {% endfor %}

    </div>

    </div>

    </div>


    <!-- ========================================================
         LIGHTS
    ======================================================== -->

    <div class="section">

    <div class="section-header"
         onclick="toggleSection(this.parentElement)">

    <div>

    <div class="section-title">
        💡 Lights
    </div>

    <div class="section-summary">
        {{stats.light_total}} lights ·
        {{stats.lights_on}} currently ON
    </div>

    </div>

    <div class="chevron">▼</div>

    </div>


    <div class="section-body">

    <div class="component-grid">

    {% for c in lights %}

    <div class="component">

    <div class="component-head"
         onclick="toggleComponent(this.parentElement)">

    <div class="component-top">

    <div>

    <div class="component-name">
        {{c.name}}
    </div>

    <div class="component-meta">
        {{c.zone or '-'}}
        {% if c.group_name %}
        · {{c.group_name}}
        {% endif %}
    </div>

    </div>

    <div class="component-arrow">▼</div>

    </div>


    <div class="component-state">

    {% if c.is_on %}

    <span class="badge good">ON</span>

    {% else %}

    <span class="badge gray">OFF</span>

    {% endif %}

    {% if c.daytime %}

    <span class="badge warn">DAYTIME</span>

    {% endif %}

    </div>

    </div>


    <div class="component-details">

    <div class="detail-grid">

    <div class="detail">
    <div class="detail-label">Zone</div>
    <div class="detail-value">{{c.zone or '-'}}</div>
    </div>

    <div class="detail">
    <div class="detail-label">Group</div>
    <div class="detail-value">{{c.group_name or '-'}}</div>
    </div>

    <div class="detail">
    <div class="detail-label">State</div>
    <div class="detail-value">{{'ON' if c.is_on else 'OFF'}}</div>
    </div>

    <div class="detail">
    <div class="detail-label">Simulator Time</div>
    <div class="detail-value">{{sim_time or '-'}}</div>
    </div>

    </div>


    {% if c.daytime and c.is_on %}

    <div class="manual-warning">
        ⚡ Electricity rule:
        this light is ON during simulator daytime.
        Turn it OFF to comply with the operating rule.
    </div>

    {% else %}

    <div class="manual-info">
        Manual ON is permitted only when the simulator is outside
        the configured daytime period.
    </div>

    {% endif %}


    <div class="control-group">

    <div class="control-label">
        Manual light control
    </div>

    <div class="controls">

    <form method="post"
          action="/maintenance/manual/light/{{c.name}}/on">

    <button
        class="btn-green"
        {% if c.is_on or c.daytime %}disabled{% endif %}>
        💡 ON
    </button>

    </form>


    <form method="post"
          action="/maintenance/manual/light/{{c.name}}/off">

    <button
        class="btn-red"
        {% if not c.is_on %}disabled{% endif %}>
        OFF
    </button>

    </form>

    </div>

    </div>

    </div>

    </div>

    {% else %}

    <div class="empty">
        No lights returned by simulator.
    </div>

    {% endfor %}

    </div>

    </div>

    </div>


    <!-- ========================================================
         GATES
    ======================================================== -->

    <div class="section">

    <div class="section-header"
         onclick="toggleSection(this.parentElement)">

    <div>

    <div class="section-title">
        🚧 Gates
    </div>

    <div class="section-summary">
        {{stats.gate_total}} gates ·
        {{stats.gates_broken}} broken
    </div>

    </div>

    <div class="chevron">▼</div>

    </div>


    <div class="section-body">

    <div class="component-grid">

    {% for c in gates %}

    <div class="component">

    <div class="component-head"
         onclick="toggleComponent(this.parentElement)">

    <div class="component-top">

    <div>

    <div class="component-name">
        {{c.name}}
    </div>

    <div class="component-meta">
        {{c.zone or 'Entrance / Exit'}}
    </div>

    </div>

    <div class="component-arrow">▼</div>

    </div>


    <div class="component-state">

    {% if c.broken %}

    <span class="badge bad">BROKEN</span>

    {% elif c.locked %}

    <span class="badge cyan">MAINTENANCE</span>

    {% else %}

    <span class="badge good">
        {{c.state}}
    </span>

    {% endif %}

    </div>

    </div>


    <div class="component-details">

    <div class="detail-grid">

    <div class="detail">
    <div class="detail-label">State</div>
    <div class="detail-value">{{c.state}}</div>
    </div>

    <div class="detail">
    <div class="detail-label">Zone</div>
    <div class="detail-value">{{c.zone or '-'}}</div>
    </div>

    </div>


    {% if c.locked %}

    <div class="manual-warning">
        🔒 Gate is under maintenance.
    </div>

    {% else %}

    <div class="control-group">

    <div class="control-label">
        Manual gate control
    </div>

    <div class="controls">

    <form method="post"
          action="/maintenance/manual/gate/{{c.name}}/open">

    <button class="btn-green">
        ↑ OPEN + HOLD
    </button>

    </form>

    <form method="post"
          action="/maintenance/manual/gate/{{c.name}}/close">

    <button class="btn-blue">
        ↓ CLOSE + HOLD
    </button>

    </form>

    <form method="post"
          action="/maintenance/manual/gate/{{c.name}}/auto">

    <button class="btn-blue">
        AUTO
    </button>

    </form>

    </div>

    </div>

    {% endif %}


    {% if c.broken %}

    <div class="control-group">

    <form method="post"
          action="/maintenance/repair/gate/{{c.name}}">

    <button class="btn-yellow">
        🔧 Repair Gate
    </button>

    </form>

    </div>

    {% endif %}

    </div>

    </div>

    {% else %}

    <div class="empty">
        No gates returned by simulator.
    </div>

    {% endfor %}

    </div>

    </div>

    </div>


    <!-- ========================================================
         PARKING SPOTS
    ======================================================== -->

    <div class="section">

    <div class="section-header"
         onclick="toggleSection(this.parentElement)">

    <div>

    <div class="section-title">
        🅿 Parking Spots
    </div>

    <div class="section-summary">
        {{stats.spot_total}} spots ·
        {{stats.occupied}} occupied ·
        {{stats.spots_broken}} broken
    </div>

    </div>

    <div class="chevron">▼</div>

    </div>


    <div class="section-body">

    <div class="component-grid">

    {% for c in spots %}

    <div class="component">

    <div class="component-head"
         onclick="toggleComponent(this.parentElement)">

    <div class="component-top">

    <div>

    <div class="component-name">
        {{c.name}}
    </div>

    <div class="component-meta">
        {{c.zone or '-'}}
    </div>

    </div>

    <div class="component-arrow">▼</div>

    </div>


    <div class="component-state">

    {% if c.broken %}

    <span class="badge bad">BROKEN</span>

    {% elif c.detected_cars > 0 %}

    <span class="badge warn">OCCUPIED</span>

    {% else %}

    <span class="badge good">FREE</span>

    {% endif %}

    </div>

    </div>


    <div class="component-details">

    <div class="detail-grid">

    <div class="detail">
    <div class="detail-label">Purpose</div>
    <div class="detail-value">{{c.purpose or '-'}}</div>
    </div>

    <div class="detail">
    <div class="detail-label">Detected Cars</div>
    <div class="detail-value">{{c.detected_cars}}</div>
    </div>

    <div class="detail">
    <div class="detail-label">Zone</div>
    <div class="detail-value">{{c.zone or '-'}}</div>
    </div>

    <div class="detail">
    <div class="detail-label">Maintenance</div>
    <div class="detail-value">
    {{'LOCKED' if c.locked else 'AVAILABLE'}}
    </div>
    </div>

    </div>


    {% if c.detected_cars > 0 %}

    <div class="manual-warning">
        🚗 Repair blocked because the simulator currently detects
        {{c.detected_cars}} car(s).
    </div>

    {% endif %}


    {% if c.broken and not c.locked and c.detected_cars == 0 %}

    <form method="post"
          action="/maintenance/repair/spot/{{c.name}}">

    <button class="btn-yellow">
        🔧 Repair Spot
    </button>

    </form>

    {% endif %}

    </div>

    </div>

    {% else %}

    <div class="empty">
        No parking spots returned by simulator.
    </div>

    {% endfor %}

    </div>

    </div>

    </div>


    <!-- ========================================================
         CO SAFETY
    ======================================================== -->

    <div class="section">

    <div class="section-header"
         onclick="toggleSection(this.parentElement)">

    <div>

    <div class="section-title">
        ☣ CO Safety
    </div>

    <div class="section-summary">
        {{zones|length}} monitored zones
    </div>

    </div>

    <div class="chevron">▼</div>

    </div>


    <div class="section-body">

    <div class="co-grid">

    {% for z in zones %}

    <div class="zone {{'danger' if z.is_danger else ''}}">

    <div class="zone-title">

    <b>{{z.name}}</b>

    <span class="badge {{'bad' if z.is_danger else 'good'}}">
        {{z.risk}}
    </span>

    </div>

    <div class="co-value">
        CO {{z.co_level if z.co_level is not none else '-'}}
    </div>

    <div class="component-meta">
        Simulator gasCarbonMonoxideLevel
    </div>


    <div style="margin-top:10px">

    {% for f in z.fans %}

    <div style="padding:7px 0;border-bottom:1px solid #26364e">

    <div style="display:flex;justify-content:space-between">

    <span>{{f.name}}</span>

    {% if f.is_on %}

    <span class="good">ON</span>

    {% else %}

    <span>OFF</span>

    {% endif %}

    </div>

    </div>

    {% else %}

    <div class="component-meta">
        No fan associated with this zone.
    </div>

    {% endfor %}

    </div>

    </div>

    {% else %}

    <div class="empty">
        No zone data available.
    </div>

    {% endfor %}

    </div>

    </div>

    </div>


    <!-- ========================================================
         MAINTENANCE QUEUE
    ======================================================== -->

    <div class="section">

    <div class="section-header"
         onclick="toggleSection(this.parentElement)">

    <div>

    <div class="section-title">
        🔧 Maintenance Queue
    </div>

    <div class="section-summary">
        {{queue|length}} components require attention
    </div>

    </div>

    <div class="chevron">▼</div>

    </div>


    <div class="section-body">

    <div class="component-grid">

    {% for c in queue %}

    <div class="component">

    <div class="component-head"
         onclick="toggleComponent(this.parentElement)">

    <div class="component-top">

    <div>

    <div class="component-name">
        {{c.name}}
    </div>

    <div class="component-meta">
        {{c.kind}} · {{c.zone or '-'}}
    </div>

    </div>

    <div class="component-arrow">▼</div>

    </div>


    <div class="component-state">

    {% if c.broken %}

    <span class="badge bad">URGENT</span>

    {% elif c.alarm_problem %}

    <span class="badge warn">PREVENTIVE</span>

    {% else %}

    <span class="badge cyan">LOCKED</span>

    {% endif %}

    </div>

    </div>


    <div class="component-details">

    <div class="detail-grid">

    <div class="detail">
    <div class="detail-label">State</div>
    <div class="detail-value">{{c.state}}</div>
    </div>

    <div class="detail">
    <div class="detail-label">Zone</div>
    <div class="detail-value">{{c.zone or '-'}}</div>
    </div>

    </div>


    {% if c.alarm_problem %}

    <div class="reason">
    <b>Predictive signal</b><br>
    {{c.alarm_problem}}
    </div>

    {% endif %}


    {% if c.locked %}

    <div class="manual-warning">
        🔒 {{c.lock_reason}}
    </div>

    {% elif c.safe %}

    <div class="manual-info">
        ✓ Repair safety interlock passed.
    </div>

    {% else %}

    <div class="manual-warning">
        ⛔ Repair blocked.<br>
        {{c.safe_reason}}
    </div>

    {% endif %}


    {% if c.safe and not c.locked and c.kind in ['spot','gate','fan'] %}

    <form method="post"
          action="/maintenance/repair/{{c.kind}}/{{c.name}}">

    <button class="btn-yellow">
        🔧
        {{'Repair Broken' if c.broken else 'Preventive Repair'}}
    </button>

    </form>

    {% endif %}

    </div>

    </div>

    {% else %}

    <div class="empty">
        No maintenance required.
    </div>

    {% endfor %}

    </div>

    </div>

    </div>


    <!-- ========================================================
         REPAIR HISTORY
    ======================================================== -->

    <div class="section">

    <div class="section-header"
         onclick="toggleSection(this.parentElement)">

    <div>

    <div class="section-title">
        📋 Repair History
    </div>

    <div class="section-summary">
        Last {{jobs|length}} repair commands
    </div>

    </div>

    <div class="chevron">▼</div>

    </div>


    <div class="section-body">

    <div class="table-wrap">

    <table>

    <tr>
    <th>Component</th>
    <th>Type</th>
    <th>Started</th>
    <th>Status</th>
    <th>Completed</th>
    </tr>

    {% for j in jobs %}

    <tr>

    <td>
    <b>{{j.component}}</b>
    <br>
    <span class="component-meta">{{j.kind}}</span>
    </td>

    <td>{{j.repair_type}}</td>

    <td>{{j.start_sim_time or j.started_at}}</td>

    <td>

    <span class="badge
    {{'good' if j.status == 'COMPLETED'
    else 'bad' if j.status == 'FAILED'
    else 'warn'}}">

    {{j.status}}

    </span>

    </td>

    <td>{{j.completed_sim_time or '-'}}</td>

    </tr>

    {% else %}

    <tr>
    <td colspan="5" class="empty">
    No repair history.
    </td>
    </tr>

    {% endfor %}

    </table>

    </div>

    </div>

    </div>


    <!-- ========================================================
         MANUAL CONTROL AUDIT
    ======================================================== -->

    <div class="section">

    <div class="section-header"
         onclick="toggleSection(this.parentElement)">

    <div>

    <div class="section-title">
        🛡 Manual Operation Audit
    </div>

    <div class="section-summary">
        Last {{manual_logs|length}} manual commands
    </div>

    </div>

    <div class="chevron">▼</div>

    </div>


    <div class="section-body">

    <div class="table-wrap">

    <table>

    <tr>
    <th>Time</th>
    <th>User</th>
    <th>Component</th>
    <th>Action</th>
    <th>Result</th>
    </tr>

    {% for a in manual_logs %}

    <tr>

    <td>{{a.created_at}}</td>

    <td>{{a.username or '-'}}</td>

    <td>{{a.component}}</td>

    <td>{{a.action}}</td>

    <td>

    <span class="badge
    {{'good' if a.result == 'SUCCESS' else 'bad'}}">

    {{a.result}}

    </span>

    </td>

    </tr>

    {% else %}

    <tr>
    <td colspan="5" class="empty">
    No manual operations recorded.
    </td>
    </tr>

    {% endfor %}

    </table>

    </div>

    </div>

    </div>


    </div>


    <script>

    function toggleSection(el){
        el.classList.toggle("open");
    }

    function toggleComponent(el){
        el.classList.toggle("open");
    }

    </script>

    </body>
    </html>
    """


    # ============================================================
    # DATABASE
    # ============================================================

    def db():
        conn = sqlite3.connect(
            str(DB_PATH),
            timeout=10
        )

        conn.row_factory = sqlite3.Row

        return conn


    def table_exists(conn, name):
        return bool(
            conn.execute(
                """
                SELECT 1
                FROM sqlite_master
                WHERE type='table'
                AND name=?
                """,
                (name,)
            ).fetchone()
        )


    def column_names(conn, table):
        if not table_exists(conn, table):
            return set()

        return {
            row[1]
            for row in conn.execute(
                f"PRAGMA table_info({table})"
            ).fetchall()
        }


    def init_maintenance_tables():

        conn = db()

        conn.executescript("""

        CREATE TABLE IF NOT EXISTS maintenance_api_snapshot(

            name TEXT,
            kind TEXT,
            zone TEXT,
            state TEXT,

            broken INTEGER,
            api_under_maintenance INTEGER,

            detected_cars INTEGER DEFAULT 0,

            purpose TEXT,
            group_name TEXT,

            is_on INTEGER DEFAULT 0,

            raw_json TEXT,
            synced_at TEXT,

            PRIMARY KEY(name,kind)
        );


        CREATE TABLE IF NOT EXISTS maintenance_alarm_snapshot(

            name TEXT PRIMARY KEY,
            problem TEXT,
            raw_json TEXT,
            synced_at TEXT

        );


        CREATE TABLE IF NOT EXISTS maintenance_zone_snapshot(

            name TEXT PRIMARY KEY,

            co_level REAL,
            risk TEXT,

            raw_json TEXT,
            synced_at TEXT

        );


        CREATE TABLE IF NOT EXISTS maintenance_meta(

            key TEXT PRIMARY KEY,
            value TEXT

        );


        CREATE TABLE IF NOT EXISTS maintenance_dashboard_jobs(

            id INTEGER PRIMARY KEY AUTOINCREMENT,

            component TEXT,
            kind TEXT,

            repair_type TEXT,

            started_at TEXT,
            start_sim_time TEXT,

            status TEXT,

            completed_sim_time TEXT,

            detail TEXT

        );


        CREATE TABLE IF NOT EXISTS maintenance_manual_log(

            id INTEGER PRIMARY KEY AUTOINCREMENT,

            created_at TEXT,

            username TEXT,

            role TEXT,

            component TEXT,

            kind TEXT,

            action TEXT,

            result TEXT,

            detail TEXT

        );

        """)

        # Migration for databases created using older version.

        cols = column_names(
            conn,
            "maintenance_api_snapshot"
        )

        if "is_on" not in cols:
            conn.execute(
                """
                ALTER TABLE maintenance_api_snapshot
                ADD COLUMN is_on INTEGER DEFAULT 0
                """
            )

        conn.commit()
        conn.close()


    # ============================================================
    # META
    # ============================================================

    def set_meta(key, value):

        conn = db()

        conn.execute(
            """
            INSERT INTO maintenance_meta(key,value)
            VALUES(?,?)

            ON CONFLICT(key)
            DO UPDATE SET value=excluded.value
            """,
            (key, str(value))
        )

        conn.commit()
        conn.close()


    def get_meta(key, default=""):

        conn = db()

        row = conn.execute(
            """
            SELECT value
            FROM maintenance_meta
            WHERE key=?
            """,
            (key,)
        ).fetchone()

        conn.close()

        if row:
            return row["value"]

        return default


    # ============================================================
    # SIMULATOR TIME
    # ============================================================

    def get_sim_time(conn=None):

        own = False

        if conn is None:
            conn = db()
            own = True

        value = ""

        if table_exists(conn, "system_state"):

            row = conn.execute(
                """
                SELECT value
                FROM system_state
                WHERE key='last_simulator_time'
                """
            ).fetchone()

            if row:
                value = row["value"] or ""

        if own:
            conn.close()

        return value


    def is_daytime(sim_time):

        """
        Lights should not operate during simulator daytime.

        If simulator time cannot be parsed, return False rather than
        inventing a daytime state.
        """

        if not sim_time:
            return False

        text = str(sim_time).strip()

        import re

        match = re.search(
            r'(\d{1,2}):(\d{2})',
            text
        )

        if not match:
            return False

        hour = int(match.group(1))

        # PARKMIND electricity rule:
        # daytime = 06:00 through 17:59
        return 6 <= hour < 18


    # ============================================================
    # SIMULATOR AUTH
    # ============================================================

    def sim_login():

        nonlocal TOKEN

        response = requests.post(
            f"{SIM_BASE}/auth/login",
            json={
                "email": SIM_USER,
                "password": SIM_PASSWORD
            },
            timeout=7
        )

        response.raise_for_status()

        body = response.json()

        TOKEN = (
            body.get("token")
            or body.get("accessToken")
            or body.get("access_token")
        )

        if not TOKEN:
            raise RuntimeError(
                "Simulator authentication returned no token"
            )

        return TOKEN


    def sim_request(method, path, **kwargs):

        nonlocal TOKEN

        if not TOKEN:
            sim_login()

        headers = kwargs.pop("headers", {})

        headers["Authorization"] = f"Bearer {TOKEN}"

        response = requests.request(
            method,
            f"{SIM_BASE}{path}",
            headers=headers,
            timeout=8,
            **kwargs
        )

        if response.status_code == 401:

            sim_login()

            headers["Authorization"] = f"Bearer {TOKEN}"

            response = requests.request(
                method,
                f"{SIM_BASE}{path}",
                headers=headers,
                timeout=8,
                **kwargs
            )

        response.raise_for_status()

        return response


    # ============================================================
    # UTILITIES
    # ============================================================

    def detected_count(value):

        if isinstance(value, list):
            return len(value)

        try:
            return int(value or 0)
        except Exception:
            return 0


    def risk_requires_fan(risk):

        return str(risk or "").strip().lower() in {
            "mid",
            "moderate",
            "high",
            "critical",
            "danger",
            "unsafe"
        }


    def record_manual(
        component,
        kind,
        action,
        result,
        detail=""
    ):

        conn = db()

        conn.execute(
            """
            INSERT INTO maintenance_manual_log(
                created_at,
                username,
                role,
                component,
                kind,
                action,
                result,
                detail
            )
            VALUES(?,?,?,?,?,?,?,?)
            """,
            (
                datetime.now().strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),

                session.get("user", ""),

                session.get("role", ""),

                component,

                kind,

                action,

                result,

                detail
            )
        )

        conn.commit()
        conn.close()


    # ============================================================
    # SNAPSHOT
    # ============================================================

    def refresh_snapshot():

        now = datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        endpoints = {

            "spot":
                "/list-parking-spots",

            "gate":
                "/list-barriers",

            "light":
                "/list-lights",

            "fan":
                "/list-exhaust-fans",
        }

        pulled = {}

        for kind, endpoint in endpoints.items():

            data = sim_request(
                "GET",
                endpoint
            ).json()

            pulled[kind] = (
                data
                if isinstance(data, list)
                else []
            )


        alarms = sim_request(
            "GET",
            "/list-alarms"
        ).json()

        if not isinstance(alarms, list):
            alarms = []


        zones = sim_request(
            "GET",
            "/list-zones"
        ).json()

        if not isinstance(zones, list):
            zones = []


        conn = db()

        conn.execute(
            "DELETE FROM maintenance_api_snapshot"
        )

        conn.execute(
            "DELETE FROM maintenance_alarm_snapshot"
        )

        conn.execute(
            "DELETE FROM maintenance_zone_snapshot"
        )


        for kind, items in pulled.items():

            for item in items:

                name = str(
                    item.get("name") or ""
                ).strip()

                if not name:
                    continue


                zone = str(
                    item.get("zoneParent") or ""
                ).strip()


                purpose = str(
                    item.get("purpose") or ""
                ).strip()


                if kind == "light":

                    broken = None
                    under = None

                else:

                    broken = int(
                        bool(
                            item.get(
                                "broken",
                                False
                            )
                        )
                    )

                    under = int(
                        bool(
                            item.get(
                                "isUnderMaintenance",
                                False
                            )
                        )
                    )


                detected = 0

                is_on = False


                if kind == "spot":

                    detected = detected_count(
                        item.get("detectedCars")
                    )

                    if purpose == "Park":

                        state = (
                            "Occupied"
                            if detected > 0
                            else "Free"
                        )

                    else:

                        state = (
                            purpose
                            or "Sensor"
                        )


                elif kind == "gate":

                    state = str(
                        item.get(
                            "state",
                            "Unknown"
                        )
                    )


                elif kind in ("fan", "light"):

                    is_on = bool(
                        item.get(
                            "isOn",
                            False
                        )
                    )

                    state = (
                        "On"
                        if is_on
                        else "Off"
                    )

                else:

                    state = "Unknown"


                conn.execute(
                    """
                    INSERT INTO maintenance_api_snapshot(
                        name,
                        kind,
                        zone,
                        state,
                        broken,
                        api_under_maintenance,
                        detected_cars,
                        purpose,
                        group_name,
                        is_on,
                        raw_json,
                        synced_at
                    )
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        name,
                        kind,
                        zone,
                        state,
                        broken,
                        under,
                        detected,
                        purpose,
                        str(
                            item.get("group") or ""
                        ),
                        int(is_on),
                        json.dumps(item),
                        now
                    )
                )


        for alarm in alarms:

            name = str(
                alarm.get("name")
                or alarm.get("Name")
                or ""
            ).strip()

            if not name:
                continue

            problem = str(
                alarm.get("problem")
                or alarm.get("Problem")
                or "Require Maintenance"
            )

            conn.execute(
                """
                INSERT INTO maintenance_alarm_snapshot(
                    name,
                    problem,
                    raw_json,
                    synced_at
                )
                VALUES(?,?,?,?)
                """,
                (
                    name,
                    problem,
                    json.dumps(alarm),
                    now
                )
            )


        for zone in zones:

            name = str(
                zone.get("name") or ""
            ).strip()

            if not name:
                continue

            conn.execute(
                """
                INSERT INTO maintenance_zone_snapshot(
                    name,
                    co_level,
                    risk,
                    raw_json,
                    synced_at
                )
                VALUES(?,?,?,?,?)
                """,
                (
                    name,
                    zone.get(
                        "gasCarbonMonoxideLevel"
                    ),
                    str(
                        zone.get(
                            "risk",
                            "Unknown"
                        )
                    ),
                    json.dumps(zone),
                    now
                )
            )


        conn.commit()
        conn.close()

        set_meta(
            "last_refresh",
            now
        )

        set_meta(
            "last_refresh_ok",
            "1"
        )

        return True


    # ============================================================
    # LIVE COMPONENT
    # ============================================================

    def live_component(kind, name):

        endpoints = {

            "spot":
                "/list-parking-spots",

            "gate":
                "/list-barriers",

            "fan":
                "/list-exhaust-fans",

            "light":
                "/list-lights",
        }

        endpoint = endpoints.get(kind)

        if not endpoint:
            return None

        items = sim_request(
            "GET",
            endpoint
        ).json()

        if not isinstance(items, list):
            return None

        for item in items:

            if str(
                item.get("name") or ""
            ) == name:

                return item

        return None


    def live_zone(zone_name):

        zones = sim_request(
            "GET",
            "/list-zones"
        ).json()

        if not isinstance(zones, list):
            return None

        for zone in zones:

            if str(
                zone.get("name") or ""
            ) == zone_name:

                return zone

        return None


    # ============================================================
    # MANUAL COMMAND
    #
    # These are the actual simulator commands.
    #
    # If your simulator uses different endpoint names,
    # set environment variables:
    #
    # PARKMIND_FAN_ON_ENDPOINT
    # PARKMIND_FAN_OFF_ENDPOINT
    # PARKMIND_LIGHT_ON_ENDPOINT
    # PARKMIND_LIGHT_OFF_ENDPOINT
    # PARKMIND_GATE_OPEN_ENDPOINT
    # PARKMIND_GATE_CLOSE_ENDPOINT
    #
    # Supported placeholders:
    # {name}
    #
    # Example:
    #
    # /exhaust-fans/{name}/on
    # ============================================================

    def manual_endpoint(kind, action, name):

        encoded = quote(
            name,
            safe=""
        )

        custom_names = {

            ("fan", "on"):
                "PARKMIND_FAN_ON_ENDPOINT",

            ("fan", "off"):
                "PARKMIND_FAN_OFF_ENDPOINT",

            ("light", "on"):
                "PARKMIND_LIGHT_ON_ENDPOINT",

            ("light", "off"):
                "PARKMIND_LIGHT_OFF_ENDPOINT",

            ("gate", "open"):
                "PARKMIND_GATE_OPEN_ENDPOINT",

            ("gate", "close"):
                "PARKMIND_GATE_CLOSE_ENDPOINT",
        }


        env_name = custom_names.get(
            (kind, action)
        )

        if env_name:

            template = os.getenv(
                env_name,
                ""
            ).strip()

            if template:

                return template.replace(
                    "{name}",
                    encoded
                )


        # Default Level 2 endpoint conventions.
        defaults = {

            ("fan", "on"):
                f"/exhaust-fans/{encoded}/on",

            ("fan", "off"):
                f"/exhaust-fans/{encoded}/off",

            ("light", "on"):
                f"/lights/{encoded}/on",

            ("light", "off"):
                f"/lights/{encoded}/off",

            ("gate", "open"):
                f"/barrier-gates/{encoded}/open",

            ("gate", "close"):
                f"/barrier-gates/{encoded}/close",
        }


        return defaults.get(
            (kind, action)
        )


    # ============================================================
    # MANUAL CONTROL SAFETY
    # ============================================================

    def manual_control(
        kind,
        name,
        action
    ):

        # --------------------------------------------------------
        # GET LIVE COMPONENT
        # --------------------------------------------------------

        item = live_component(
            kind,
            name
        )

        if not item:

            return False, (
                f"{name} was not found "
                "in the live simulator API."
            )


        # --------------------------------------------------------
        # MAINTENANCE LOCK
        # --------------------------------------------------------

        if bool(
            item.get(
                "isUnderMaintenance",
                False
            )
        ):

            return False, (
                f"{name} is currently under "
                "maintenance. Manual operation "
                "is disabled."
            )


        # --------------------------------------------------------
        # FAN
        # --------------------------------------------------------

        if kind == "fan":

            zone = str(
                item.get(
                    "zoneParent",
                    ""
                )
            )

            is_on = bool(
                item.get(
                    "isOn",
                    False
                )
            )


            zone_data = live_zone(
                zone
            )


            risk = ""

            if zone_data:

                risk = str(
                    zone_data.get(
                        "risk",
                        ""
                    )
                )


            # Turning OFF during dangerous CO conditions
            # is blocked.

            if (
                action == "off"
                and risk_requires_fan(risk)
            ):

                return False, (
                    f"Manual OFF blocked: "
                    f"{zone} has CO risk "
                    f"{risk}. Healthy ventilation "
                    "must remain available."
                )


            if action == "on" and is_on:

                return False, (
                    f"{name} is already ON."
                )


            if action == "off" and not is_on:

                return False, (
                    f"{name} is already OFF."
                )


        # --------------------------------------------------------
        # LIGHT
        # --------------------------------------------------------

        if kind == "light":

            is_on = bool(
                item.get(
                    "isOn",
                    False
                )
            )

            sim_time = get_sim_time()

            daytime = is_daytime(
                sim_time
            )


            if (
                action == "on"
                and daytime
            ):

                return False, (
                    "Manual light ON blocked: "
                    "lights should not operate "
                    "during simulator daytime."
                )


            if action == "on" and is_on:

                return False, (
                    f"{name} is already ON."
                )


            if action == "off" and not is_on:

                return False, (
                    f"{name} is already OFF."
                )


        # --------------------------------------------------------
        # GATE
        # --------------------------------------------------------

        if kind == "gate":

            state = str(
                item.get(
                    "state",
                    ""
                )
            ).lower()

            if action == "auto":
                with state_lock:
                    manual_overrides["gate"].pop(name, None)
                    manual_gate_closed.discard(name)
                ensure_keep_open_gates()
                return True, (
                    f"{name} returned to AUTO control."
                )

            if action == "close" and name == ENTRY_GATE:
                return False, (
                    f"{name} is the API-discovered entry gate and cannot be held CLOSED because that would break the simulator arrival path."
                )

            if action == "open":
                with state_lock:
                    manual_overrides["gate"][name] = True
                    manual_gate_closed.discard(name)
                if state not in ("open", "opening"):
                    if not open_gate(name, manual=True):
                        return False, f"Could not open {name}."
                return True, (
                    f"{name} is now MANUAL OPEN; automation cannot close it until AUTO or CLOSE is selected."
                )

            if action == "close":
                with state_lock:
                    manual_overrides["gate"][name] = False
                    manual_gate_closed.add(name)
                if state not in ("closed", "closing"):
                    if not close_gate(name, force=True, manual=True):
                        return False, f"Could not close {name}."
                return True, (
                    f"{name} is now MANUAL CLOSED; automation cannot open it until AUTO or OPEN is selected."
                )


        # --------------------------------------------------------
        # SEND COMMAND
        # --------------------------------------------------------

        endpoint = manual_endpoint(
            kind,
            action,
            name
        )


        if not endpoint:

            return False, (
                f"No manual {action} endpoint "
                f"is configured for {kind}."
            )


        try:

            response = sim_request(
                "POST",
                endpoint
            )

        except requests.HTTPError as exc:

            status = ""

            if exc.response is not None:

                status = (
                    f"HTTP {exc.response.status_code}"
                )


            return False, (
                f"Simulator rejected manual "
                f"{action} command for {name}"
                f"{' (' + status + ')' if status else ''}."
            )


        except Exception as exc:

            return False, (
                f"Manual command failed: {exc}"
            )


        return True, (
            f"Simulator accepted "
            f"{action.upper()} command for {name}."
        )


    # ============================================================
    # SIGNED EVENT RECONCILIATION
    # ============================================================

    def reconcile_signed_events():

        conn = db()

        if not table_exists(
            conn,
            "events"
        ):

            conn.close()
            return


        cols = column_names(
            conn,
            "events"
        )

        if not {
            "event_class",
            "payload"
        }.issubset(cols):

            conn.close()
            return


        rows = conn.execute(
            """
            SELECT
                event_class,
                server_time,
                payload
            FROM events
            WHERE event_class IN(
                'component_broken',
                'component_fixed'
            )
            ORDER BY id ASC
            """
        ).fetchall()


        latest = {}


        for row in rows:

            try:

                payload = json.loads(
                    row["payload"] or "{}"
                )

            except Exception:

                continue


            name = str(
                payload.get("Name")
                or ""
            ).strip()


            if not name:
                continue


            latest[name] = {

                "event_class":
                    row["event_class"],

                "server_time":
                    row["server_time"]
                    or payload.get(
                        "ServerDateTime"
                    )
                    or "",
            }


        for name, event in latest.items():

            if event["event_class"] == "component_broken":

                conn.execute(
                    """
                    UPDATE maintenance_api_snapshot
                    SET broken=1
                    WHERE name=?
                    AND broken IS NOT NULL
                    """,
                    (name,)
                )


            elif event["event_class"] == "component_fixed":

                conn.execute(
                    """
                    UPDATE maintenance_api_snapshot
                    SET
                        broken=0,
                        api_under_maintenance=0
                    WHERE name=?
                    AND broken IS NOT NULL
                    """,
                    (name,)
                )


                conn.execute(
                    """
                    DELETE FROM maintenance_alarm_snapshot
                    WHERE name=?
                    """,
                    (name,)
                )


                conn.execute(
                    """
                    UPDATE maintenance_dashboard_jobs
                    SET
                        status='COMPLETED',
                        completed_sim_time=?
                    WHERE component=?
                    AND status='COMMAND_SENT'
                    """,
                    (
                        event["server_time"],
                        name
                    )
                )


        conn.commit()
        conn.close()


    # ============================================================
    # REPAIR SAFETY
    # ============================================================

    def safe_decision(
        component,
        zones_by_name
    ):

        if component["locked"]:

            return (
                False,
                component["lock_reason"]
            )


        kind = component["kind"]


        if kind == "spot":

            if component["purpose"] != "Park":

                return (
                    False,
                    "This is not a normal parking bay."
                )


            if int(
                component["detected_cars"] or 0
            ) > 0:

                return (
                    False,
                    "Parking spot is occupied."
                )


            return (
                True,
                "Parking spot is free."
            )


        if kind == "gate":

            if component["broken"]:

                return (
                    True,
                    "Gate is broken."
                )


            if str(
                component["state"]
            ).lower() != "closed":

                return (
                    False,
                    "Gate must be CLOSED before preventive repair."
                )


            return (
                True,
                "Gate is safely CLOSED."
            )


        if kind == "fan":

            if component["broken"]:

                return (
                    True,
                    "Fan is broken."
                )


            zone = zones_by_name.get(
                component["zone"]
            )


            if (
                zone
                and
                risk_requires_fan(
                    zone["risk"]
                )
            ):

                return (
                    False,
                    f"CO risk is {zone['risk']}; ventilation must remain available."
                )


            if component["is_on"]:

                return (
                    False,
                    "Fan must be OFF before preventive repair."
                )


            return (
                True,
                "Fan is OFF and ventilation is not currently required."
            )


        return (
            False,
            "No documented repair operation."
        )


    # ============================================================
    # ROUTES
    # ============================================================

    @maintenance_bp.route(
        "/refresh",
        methods=["POST"]
    )
    def refresh():

        if not session.get("user"):
            return redirect("/login")


        if session.get("role") not in {
            "Maintenance",
            "Admin"
        }:
            return redirect("/")


        try:

            refresh_snapshot()

            flash(
                "Maintenance snapshot refreshed from simulator."
            )

        except Exception as exc:

            set_meta(
                "last_refresh_ok",
                "0"
            )

            flash(
                f"Simulator refresh failed: {exc}"
            )


        return redirect(
            "/maintenance/"
        )


    # ============================================================
    # MANUAL FAN
    # ============================================================

    @maintenance_bp.route(
        "/manual/fan/<name>/<action>",
        methods=["POST"]
    )
    def manual_fan(name, action):

        if not session.get("user"):
            return redirect("/login")


        if session.get("role") not in {
            "Maintenance",
            "Admin"
        }:
            return redirect("/")


        if action not in {
            "on",
            "off"
        }:

            flash(
                "Invalid fan command."
            )

            return redirect(
                "/maintenance/"
            )


        success, detail = manual_control(
            "fan",
            name,
            action
        )


        record_manual(
            name,
            "fan",
            f"MANUAL_{action.upper()}",
            "SUCCESS" if success else "FAILED",
            detail
        )


        if success:

            flash(
                f"🌀 {detail}"
            )

            # Refresh real state immediately.
            try:
                refresh_snapshot()
            except Exception:
                pass

        else:

            flash(
                f"⛔ {detail}"
            )


        return redirect(
            "/maintenance/"
        )


    # ============================================================
    # MANUAL LIGHT
    # ============================================================

    @maintenance_bp.route(
        "/manual/light/<name>/<action>",
        methods=["POST"]
    )
    def manual_light(name, action):

        if not session.get("user"):
            return redirect("/login")


        if session.get("role") not in {
            "Maintenance",
            "Admin"
        }:
            return redirect("/")


        if action not in {
            "on",
            "off"
        }:

            flash(
                "Invalid light command."
            )

            return redirect(
                "/maintenance/"
            )


        success, detail = manual_control(
            "light",
            name,
            action
        )


        record_manual(
            name,
            "light",
            f"MANUAL_{action.upper()}",
            "SUCCESS" if success else "FAILED",
            detail
        )


        if success:

            flash(
                f"💡 {detail}"
            )

            try:
                refresh_snapshot()
            except Exception:
                pass

        else:

            flash(
                f"⛔ {detail}"
            )


        return redirect(
            "/maintenance/"
        )


    # ============================================================
    # MANUAL GATE
    # ============================================================

    @maintenance_bp.route(
        "/manual/gate/<name>/<action>",
        methods=["POST"]
    )
    def manual_gate(name, action):

        if not session.get("user"):
            return redirect("/login")


        if session.get("role") not in {
            "Maintenance",
            "Admin"
        }:
            return redirect("/")


        if action not in {
            "open",
            "close",
            "auto"
        }:

            flash(
                "Invalid gate command."
            )

            return redirect(
                "/maintenance/"
            )


        success, detail = manual_control(
            "gate",
            name,
            action
        )


        record_manual(
            name,
            "gate",
            f"MANUAL_{action.upper()}",
            "SUCCESS" if success else "FAILED",
            detail
        )


        if success:

            flash(
                f"🚧 {detail}"
            )

            try:
                refresh_snapshot()
            except Exception:
                pass

        else:

            flash(
                f"⛔ {detail}"
            )


        return redirect(
            "/maintenance/"
        )


    # ============================================================
    # REPAIR
    # ============================================================

    @maintenance_bp.route(
        "/repair/<kind>/<name>",
        methods=["POST"]
    )
    def repair(kind, name):

        if not session.get("user"):
            return redirect("/login")


        if session.get("role") not in {
            "Maintenance",
            "Admin"
        }:
            return redirect("/")


        if kind not in {
            "spot",
            "gate",
            "fan"
        }:

            flash(
                "Repair blocked: unsupported component."
            )

            return redirect(
                "/maintenance/"
            )


        try:

            item = live_component(
                kind,
                name
            )


            if not item:

                flash(
                    f"Repair blocked: {name} was not found."
                )

                return redirect(
                    "/maintenance/"
                )


            if bool(
                item.get(
                    "isUnderMaintenance",
                    False
                )
            ):

                flash(
                    f"Repair blocked: {name} is already under maintenance."
                )

                return redirect(
                    "/maintenance/"
                )


            broken = bool(
                item.get(
                    "broken",
                    False
                )
            )


            zone = str(
                item.get(
                    "zoneParent",
                    ""
                )
            )


            if kind == "spot":

                purpose = str(
                    item.get(
                        "purpose",
                        ""
                    )
                )

                cars = detected_count(
                    item.get(
                        "detectedCars"
                    )
                )


                if purpose != "Park":

                    flash(
                        f"Repair blocked: {name} is {purpose}."
                    )

                    return redirect(
                        "/maintenance/"
                    )


                if cars > 0:

                    flash(
                        f"Repair blocked: {name} has {cars} detected car(s)."
                    )

                    return redirect(
                        "/maintenance/"
                    )


            elif kind == "gate":

                state = str(
                    item.get(
                        "state",
                        ""
                    )
                )


                if (
                    not broken
                    and
                    state.lower() != "closed"
                ):

                    flash(
                        f"Repair blocked: {name} must be CLOSED."
                    )

                    return redirect(
                        "/maintenance/"
                    )


            elif kind == "fan":

                is_on = bool(
                    item.get(
                        "isOn",
                        False
                    )
                )


                if not broken:

                    zone_data = live_zone(
                        zone
                    )


                    if (
                        zone_data
                        and
                        risk_requires_fan(
                            zone_data.get("risk")
                        )
                    ):

                        flash(
                            f"Repair blocked: {zone} requires ventilation."
                        )

                        return redirect(
                            "/maintenance/"
                        )


                    if is_on:

                        flash(
                            f"Repair blocked: {name} is ON. Turn it OFF first."
                        )

                        return redirect(
                            "/maintenance/"
                        )


            endpoint = {

                "spot":
                    f"/parking-spots/{quote(name, safe='')}/repair",

                "gate":
                    f"/barrier-gates/{quote(name, safe='')}/repair",

                "fan":
                    f"/exhaust-fans/{quote(name, safe='')}/repair",

            }[kind]


            conn = db()


            alarm = conn.execute(
                """
                SELECT problem
                FROM maintenance_alarm_snapshot
                WHERE name=?
                """,
                (name,)
            ).fetchone()


            sim_time = get_sim_time(
                conn
            )


            repair_type = (
                "CORRECTIVE"
                if broken
                else
                "PREVENTIVE"
                if alarm
                else
                "MANUAL"
            )


            existing = conn.execute(
                """
                SELECT 1
                FROM maintenance_dashboard_jobs
                WHERE component=?
                AND status='COMMAND_SENT'
                """,
                (name,)
            ).fetchone()


            if existing:

                conn.close()

                flash(
                    f"Repair already pending for {name}."
                )

                return redirect(
                    "/maintenance/"
                )


            conn.close()


            sim_request(
                "POST",
                endpoint
            )


            conn = db()


            conn.execute(
                """
                INSERT INTO maintenance_dashboard_jobs(
                    component,
                    kind,
                    repair_type,
                    started_at,
                    start_sim_time,
                    status,
                    detail
                )
                VALUES(?,?,?,?,?,?,?)
                """,
                (
                    name,
                    kind,
                    repair_type,

                    datetime.now().strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),

                    sim_time or None,

                    "COMMAND_SENT",

                    "Simulator accepted repair command"
                )
            )


            conn.commit()
            conn.close()


            flash(
                f"🔧 {repair_type.title()} repair command accepted for {name}."
            )


        except Exception as exc:

            flash(
                f"Repair command failed: {exc}"
            )


        return redirect(
            "/maintenance/"
        )


    # ============================================================
    # DASHBOARD
    # ============================================================

    @maintenance_bp.route("/")
    def dashboard():

        if not session.get("user"):
            return redirect("/login")


        if session.get("role") not in {
            "Maintenance",
            "Admin"
        }:
            return redirect("/")


        init_maintenance_tables()


        conn = db()


        snapshot_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM maintenance_api_snapshot
            """
        ).fetchone()[0]


        conn.close()


        if snapshot_count == 0:

            try:

                refresh_snapshot()

            except Exception:

                set_meta(
                    "last_refresh_ok",
                    "0"
                )


        reconcile_signed_events()


        conn = db()


        sim_time = get_sim_time(
            conn
        )


        rows = [

            dict(row)

            for row in conn.execute(
                """
                SELECT
                    s.*,
                    a.problem AS alarm_problem

                FROM maintenance_api_snapshot s

                LEFT JOIN maintenance_alarm_snapshot a
                    ON a.name=s.name

                ORDER BY
                    s.kind,
                    s.zone,
                    s.name
                """
            ).fetchall()
        ]


        zones = [

            dict(row)

            for row in conn.execute(
                """
                SELECT *
                FROM maintenance_zone_snapshot
                ORDER BY name
                """
            ).fetchall()
        ]


        zones_by_name = {
            z["name"]: z
            for z in zones
        }


        pending_names = {

            row["component"]

            for row in conn.execute(
                """
                SELECT component
                FROM maintenance_dashboard_jobs
                WHERE status='COMMAND_SENT'
                """
            ).fetchall()
        }


        components = []


        for c in rows:

            c["broken"] = (
                None
                if c["broken"] is None
                else bool(c["broken"])
            )


            c["api_under_maintenance"] = (

                None
                if c["api_under_maintenance"] is None

                else bool(
                    c["api_under_maintenance"]
                )
            )


            c["is_on"] = bool(
                c.get("is_on", 0)
            )


            c["locked"] = (

                bool(
                    c["api_under_maintenance"]
                )

                or
                c["name"] in pending_names
            )


            if c["api_under_maintenance"]:

                c["lock_reason"] = (
                    "Simulator reports component "
                    "under maintenance."
                )

            elif c["name"] in pending_names:

                c["lock_reason"] = (
                    "Repair command accepted; "
                    "waiting for component_fixed."
                )

            else:

                c["lock_reason"] = ""


            zone = zones_by_name.get(
                c["zone"]
            )


            c["co_level"] = (
                zone.get("co_level")
                if zone
                else None
            )


            c["risk"] = (
                zone.get("risk")
                if zone
                else "Unknown"
            )


            c["co_danger"] = (
                c["kind"] == "fan"
                and
                risk_requires_fan(
                    c["risk"]
                )
            )


            safe, safe_reason = safe_decision(
                c,
                zones_by_name
            )


            c["safe"] = safe
            c["safe_reason"] = safe_reason


            components.append(c)


        spots = [
            c
            for c in components
            if c["kind"] == "spot"
        ]


        gates = [
            c
            for c in components
            if c["kind"] == "gate"
        ]


        fans = [
            c
            for c in components
            if c["kind"] == "fan"
        ]


        lights = [
            c
            for c in components
            if c["kind"] == "light"
        ]


        current_daytime = is_daytime(
            sim_time
        )


        for light in lights:

            light["daytime"] = current_daytime


        queue = [

            c

            for c in components

            if (
                c["broken"]
                or
                c["alarm_problem"]
                or
                c["locked"]
            )
        ]


        queue.sort(
            key=lambda c: (
                0 if c["broken"]
                else
                1 if c["alarm_problem"]
                else 2,

                c["name"]
            )
        )


        zone_cards = []


        for z in zones:

            z = dict(z)

            z["is_danger"] = (
                risk_requires_fan(
                    z["risk"]
                )
            )


            z["fans"] = [

                f

                for f in fans

                if f["zone"] == z["name"]
            ]


            zone_cards.append(z)


        jobs = [

            dict(row)

            for row in conn.execute(
                """
                SELECT *
                FROM maintenance_dashboard_jobs
                ORDER BY id DESC
                LIMIT 30
                """
            ).fetchall()
        ]


        manual_logs = [

            dict(row)

            for row in conn.execute(
                """
                SELECT *
                FROM maintenance_manual_log
                ORDER BY id DESC
                LIMIT 30
                """
            ).fetchall()
        ]


        stats = {

            "broken":
                sum(
                    1
                    for c in components
                    if c["broken"] is True
                ),

            "preventive":
                sum(
                    1
                    for c in components
                    if (
                        c["alarm_problem"]
                        and
                        not c["broken"]
                        and
                        not c["locked"]
                    )
                ),

            "locked":
                sum(
                    1
                    for c in components
                    if c["locked"]
                ),

            "fans_on":
                sum(
                    1
                    for c in fans
                    if c["is_on"]
                ),

            "fan_total":
                len(fans),

            "fans_broken":
                sum(
                    1
                    for c in fans
                    if c["broken"]
                ),

            "light_total":
                len(lights),

            "lights_on":
                sum(
                    1
                    for c in lights
                    if c["is_on"]
                ),

            "gate_total":
                len(gates),

            "gates_broken":
                sum(
                    1
                    for c in gates
                    if c["broken"]
                ),

            "spot_total":
                len(spots),

            "occupied":
                sum(
                    1
                    for c in spots
                    if int(
                        c["detected_cars"] or 0
                    ) > 0
                ),

            "spots_broken":
                sum(
                    1
                    for c in spots
                    if c["broken"]
                ),
        }


        conn.close()


        return render_template_string(

            HTML,

            online=(
                get_meta(
                    "last_refresh_ok",
                    "0"
                ) == "1"
            ),

            last_refresh=get_meta(
                "last_refresh",
                ""
            ),

            sim_time=sim_time,

            stats=stats,

            fans=fans,

            lights=lights,

            gates=gates,

            spots=spots,

            zones=zone_cards,

            queue=queue,

            jobs=jobs,

            manual_logs=manual_logs,
        )
    return maintenance_bp


maintenance_bp = _build_maintenance_blueprint()
app.register_blueprint(maintenance_bp)

# =====================================================================
# MAIN
# =====================================================================
if __name__ == "__main__":
    init_db()

    # Start background watchdogs (innovations #4 + #11)
    threading.Thread(target=reservation_ttl_watchdog, daemon=True).start()
    threading.Thread(target=stuck_state_watchdog, daemon=True).start()
    threading.Thread(target=entry_transit_watchdog, daemon=True).start()
    threading.Thread(target=close_idle_gates, daemon=True).start()
    threading.Thread(target=exit_stuck_alert_watchdog, daemon=True).start()
    threading.Thread(target=maintenance_watchdog, daemon=True).start()
    threading.Thread(target=light_policy_watchdog, daemon=True).start()

    try:
        sim_login()
        sync_state()
        ensure_keep_open_gates()
        # One API-triggered test webhook establishes ServerDateTime immediately,
        # allowing morning/night lighting policy without polling or wall-clock guesses.
        try:
            sim_request("GET", "/test")
            log_decision("", "CLOCK_SYNC_REQUEST", "Requested one startup test webhook for simulator time")
        except Exception as clock_error:
            log_decision("", "CLOCK_SYNC_SKIPPED", str(clock_error))
        if not spots:
            print("[STARTUP] Simulator is connected; Level 2 components are not loaded yet.")
            print("[STARTUP] That's OK — the first arrival will trigger a safe sync.")
    except Exception as e:
        print("[STARTUP] Simulator not ready yet:", e)
        print("[STARTUP] Start the simulator; PARKMIND will safely sync on the first arrival.")

    print("\n============================================")
    print("  PARKMIND v3.1 — Level-2 Compliance + Preserved Parking Workflow")
    print("  Team: Pretty Little Hackers")
    print("  Dashboard: http://127.0.0.1:8000")
    print("  Webhook:   http://127.0.0.1:8000/webhook")
    print("  Login:     admin/admin · operator/operator · maintenance/maintenance")
    print("  Maintenance: http://127.0.0.1:8000/maintenance/")
    print("  API-derived entry alias:", ENTRY_GATE, "(derived from /list-barriers)")
    print("  API-derived exit alias: ", EXIT_GATE, "(derived from /list-barriers)")
    print("  Entry control: ZONE-SPILLOVER (busy first zone -> next zone; no internal pile-up)")
    print("  Exit control: TWO-STAGE + HARD PAYMENT LOCK (no payment = no leavepark)")
    print("  Entry spawn guard: HARD-LOCKED OPEN (prevents A -> P2 no-path errors)")
    print("  Gate retries: debounced (will not spam OPEN while already Opening/Open)")
    print("============================================\n")

    app.run(host="0.0.0.0", port=WEB_PORT, threaded=True)
