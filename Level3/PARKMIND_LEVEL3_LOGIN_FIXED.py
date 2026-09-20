# =====================================================================
#  PARKMIND v3.7  —  LEVEL 3 AIRPORT / API-SAFE ENTRY ROUTING + FULL-DATABASE Edition
#  Team: Pretty Little Hackers  |  Level: 3 — Airport Scale
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
#  [16] Durable Serialized Event Queue — burst webhooks are persisted first,
#       then processed by a bounded worker instead of spawning unlimited threads.
#  [17] Parking-Network Security Ledger — invalid, duplicate, replayed and
#       tampered requests are rejected/ignored and shown on a dedicated admin page.
#  [18] Traffic Optimisation — SQLite WAL + busy timeout, HTTP connection pooling,
#       bounded webhook back-pressure and slower dashboard refresh under load.
#  [19] Component Availability Summary — available/broken/maintenance totals for
#       spots, gates, fans and lights are visible at a glance.
#  [20] Full-State Database Persistence — components, queues, reservations,
#       overrides, quarantines, routing locks, counters and runtime recovery state
#       are checkpointed to SQLite and restored after controller restart.
#  [21] Defensive Event Validation + Double-Parking Detection — malformed vehicle/
#       component events are rejected safely; impossible one-car/two-bay states are
#       alerted and both affected bays are quarantined for operator review.
#  [22] Debounced Runtime Checkpointing — raw events remain durable immediately, while
#       expensive full-state checkpoints are coalesced during bursts and persistence
#       failures are isolated from successful event processing.
#  [23] Airport Manual Control Centre — every API-discovered gate, fan and light can
#       be overridden from a zone-aware GUI; commands are safety-checked, audited
#       to SQLite and immediately included in the durable runtime checkpoint.
#  [24] API-Safe Entry Admission — a car is assigned only to a zone whose gate
#       crossing is free; required barriers must report Open before the documented
#       /car/{plate}/goto/{parkingSpot} command is sent. Busy zones spill forward.
# =====================================================================

from flask import Flask, request, jsonify, render_template_string, redirect, url_for, session, Response
import requests
import sqlite3
import threading
import json
import math
import re
import hashlib
import hmac
import queue
import csv
import io
import time
import random
import os
from datetime import datetime, timedelta
from urllib.parse import quote
from collections import defaultdict, Counter
from requests.adapters import HTTPAdapter

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
# Level 3 may have several perimeter entrances and the API does not map each
# EntrySpot to one specific barrier. PARKMIND therefore keeps every healthy AUTO
# perimeter barrier open so the simulator can spawn at any available entrance.
# Payment security does NOT depend on a closed barrier: the hard paid-exit
# interlock refuses to issue leavepark until a verified payment_made webhook.
KEEP_ENTRY_GATE_OPEN = True
KEEP_OPEN_GATES_ENV = [g.strip() for g in os.getenv("PARKMIND_KEEP_OPEN_GATES", "").split(",") if g.strip()]

# Level-2 policy settings. These are controller policies, not simulator data.
DAY_START_HOUR = int(os.getenv("PARKMIND_DAY_START_HOUR", "6"))
DAY_END_HOUR = int(os.getenv("PARKMIND_DAY_END_HOUR", "18"))
PREVENTIVE_MAINTENANCE_HEALTH = int(os.getenv("PARKMIND_PREVENTIVE_HEALTH", "15"))

WEB_PORT = 8000

# Level-3 airport traffic hardening.  The webhook endpoint does only validation,
# durable persistence and queue admission; expensive event handling happens away
# from the HTTP request thread so bursts cannot freeze the dashboard.
MAX_WEBHOOK_BYTES = int(os.getenv("PARKMIND_MAX_WEBHOOK_BYTES", str(256 * 1024)))
EVENT_QUEUE_MAX = int(os.getenv("PARKMIND_EVENT_QUEUE_MAX", "5000"))
EVENT_BATCH_DELAY_SEC = float(os.getenv("PARKMIND_EVENT_BATCH_DELAY_SEC", "0.025"))
EVENT_DB_REFILL_SEC = float(os.getenv("PARKMIND_EVENT_DB_REFILL_SEC", "0.5"))
DASHBOARD_REFRESH_MS = int(os.getenv("PARKMIND_DASHBOARD_REFRESH_MS", "5000"))
SECURITY_PAYLOAD_PREVIEW = int(os.getenv("PARKMIND_SECURITY_PREVIEW", "800"))
SECURITY_LOG_MAX_ROWS = int(os.getenv("PARKMIND_SECURITY_LOG_MAX_ROWS", "5000"))
# Full-database persistence: memory remains a fast cache, SQLite is the durable copy.
RUNTIME_PERSIST_INTERVAL_SEC = float(os.getenv("PARKMIND_RUNTIME_PERSIST_INTERVAL_SEC", "1.0"))
# Burst optimisation: do not perform a full component/topology checkpoint after every
# event. A checkpoint is forced after this time OR this many processed events.
RUNTIME_EVENT_PERSIST_DEBOUNCE_SEC = float(os.getenv("PARKMIND_EVENT_PERSIST_DEBOUNCE_SEC", "0.25"))
RUNTIME_EVENT_PERSIST_MAX_EVENTS = int(os.getenv("PARKMIND_EVENT_PERSIST_MAX_EVENTS", "250"))
RUNTIME_HISTORY_MAX_ROWS = int(os.getenv("PARKMIND_RUNTIME_HISTORY_MAX_ROWS", "1000"))
COMPONENT_HISTORY_MAX_ROWS = int(os.getenv("PARKMIND_COMPONENT_HISTORY_MAX_ROWS", "20000"))
VERBOSE_WEBHOOK_LOG = os.getenv("PARKMIND_VERBOSE_WEBHOOK", "0").strip().lower() in ("1", "true", "yes")

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
SUSPICIOUS_PAYMENT_MAX_REPROMPTS = int(os.getenv("PARKMIND_PAYMENT_REPROMPTS", "2"))
SENSOR_QUARANTINE_SEC = int(os.getenv("PARKMIND_SENSOR_QUARANTINE_SEC", "30"))
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
ENTRY_TRANSIT_TIMEOUT_SEC = int(os.getenv("PARKMIND_TRANSIT_RETRY_SEC", "15"))  # slow airport routes/webhooks need more grace
ENTRY_TRANSIT_LOST_SEC = int(os.getenv("PARKMIND_TRANSIT_LOST_SEC", "240"))   # release reservation only after a true long transit
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
# Keep the original session key as the default so existing PARKMIND behaviour is
# preserved, while allowing deployment to override it safely.
app.secret_key = os.getenv("PARKMIND_FLASK_SECRET", "pretty-little-hackers-level1-v2")
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
)

token = None
token_lock = threading.Lock()

# Per-thread pooled HTTP sessions avoid a new TCP connection for every simulator
# API call while remaining safe when several controller threads make calls.
_http_local = threading.local()

def get_http_session():
    sess = getattr(_http_local, "session", None)
    if sess is None:
        sess = requests.Session()
        adapter = HTTPAdapter(pool_connections=20, pool_maxsize=50, max_retries=0, pool_block=False)
        sess.mount("http://", adapter)
        sess.mount("https://", adapter)
        _http_local.session = sess
    return sess

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
gate_topology_roles = defaultdict(set)  # gate name -> {PERIMETER or zone names}; detects ambiguous duplicate IDs

# Local operator intent is not simulator state. AUTO means PARKMIND policies may
# control that component; ON/OFF means the operator explicitly overrode it.
manual_overrides = {"gate": {}, "fan": {}, "light": {}, "light_group": {}}
maintenance_requested = set()
# Level-3 local safety quarantine for parking-bay sensor abnormalities.
# The simulator may not expose a dedicated "maintenance mode" POST; this
# cache flag therefore removes the bay from allocation immediately while a
# bounded re-check timer waits for the sensor to settle.
sensor_quarantined_spots = {}   # spot -> {reason, since, token}
suspicious_payment_retries = defaultdict(int)
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
    "webhooks_unsigned_accepted": 0,
    "webhooks_rejected_sig": 0,
    "sequence_gaps_detected": 0,
    "auto_recoveries": 0,
    "anomalies_flagged": 0,
    "gate_failovers": 0,
    "sensor_quarantines": 0,
    "payment_reprompts": 0,
    "events_accepted": 0,
    "event_queue_peak": 0,
    "event_queue_spill_to_db": 0,
}

# Track last seen SequenceId for gap detection
last_sequence_id = None
sequence_lock = threading.Lock()

# Durable bounded event-ingestion pipeline. PriorityQueue uses SequenceId where
# available, so simultaneous HTTP requests are drained in simulator order after a
# tiny batching window. One worker intentionally serializes state transitions;
# timers/API calls created by the existing lifecycle remain unchanged.
event_queue = queue.PriorityQueue(maxsize=EVENT_QUEUE_MAX)
event_enqueue_counter = 0
event_enqueue_lock = threading.Lock()
event_worker_started = False
event_worker_start_lock = threading.Lock()
last_processed_sequence = None

# Full runtime persistence is deliberately serialized so the periodic watchdog and
# event worker never compete with each other for the same expensive checkpoint.
runtime_persist_lock = threading.RLock()
runtime_persist_meta_lock = threading.Lock()
last_runtime_persist_monotonic = 0.0
events_since_runtime_persist = 0


# =====================================================================
# DATABASE
# =====================================================================
def db():
    # WAL lets dashboard readers continue while webhook/event workers are writing.
    # busy_timeout absorbs short write bursts instead of surfacing "database locked".
    conn = sqlite3.connect(os.getenv("PARKMIND_LEVEL2_DB", "parkmind_level2_v6.db"), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
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
        payload TEXT,
        received_at TEXT,
        request_fingerprint TEXT,
        processing_status TEXT DEFAULT 'RECEIVED',
        processed_at TEXT,
        processing_error TEXT
    );

    CREATE TABLE IF NOT EXISTS network_security_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        received_at TEXT,
        category TEXT,
        event_id TEXT,
        sequence_id INTEGER,
        event_class TEXT,
        remote_addr TEXT,
        request_fingerprint TEXT,
        detail TEXT,
        payload_preview TEXT,
        payload_full TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_security_received_at ON network_security_log(received_at DESC);
    CREATE INDEX IF NOT EXISTS idx_security_category ON network_security_log(category);

    -- Durable copy of the complete controller runtime state. The application
    -- still uses RAM as a fast cache, but restart recovery never depends on RAM.
    CREATE TABLE IF NOT EXISTS runtime_state (
        state_key TEXT PRIMARY KEY,
        state_json TEXT NOT NULL,
        state_hash TEXT,
        updated_at TEXT,
        reason TEXT
    );

    CREATE TABLE IF NOT EXISTS runtime_state_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT,
        reason TEXT,
        state_hash TEXT,
        state_json TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_runtime_history_created ON runtime_state_history(created_at DESC);

    -- Query-friendly current component state used for health/availability reports.
    CREATE TABLE IF NOT EXISTS component_state (
        component_type TEXT NOT NULL,
        name TEXT NOT NULL,
        zone_parent TEXT,
        broken INTEGER DEFAULT 0,
        under_maintenance INTEGER DEFAULT 0,
        available INTEGER DEFAULT 1,
        occupied INTEGER DEFAULT 0,
        state_hash TEXT,
        state_json TEXT NOT NULL,
        updated_at TEXT,
        PRIMARY KEY(component_type, name)
    );
    CREATE INDEX IF NOT EXISTS idx_component_state_health
        ON component_state(component_type, broken, under_maintenance, available);
    CREATE INDEX IF NOT EXISTS idx_component_state_zone ON component_state(zone_parent);

    -- History is written only when a component's JSON state actually changes.
    CREATE TABLE IF NOT EXISTS component_state_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        changed_at TEXT,
        component_type TEXT,
        name TEXT,
        zone_parent TEXT,
        state_hash TEXT,
        state_json TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_component_history_lookup
        ON component_state_history(component_type, name, changed_at DESC);

    -- Normalized durable view of controller queues for recovery/auditing.
    CREATE TABLE IF NOT EXISTS controller_queue_state (
        queue_name TEXT NOT NULL,
        position INTEGER NOT NULL,
        item_json TEXT NOT NULL,
        updated_at TEXT,
        PRIMARY KEY(queue_name, position)
    );

    CREATE TABLE IF NOT EXISTS persistence_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        saved_at TEXT,
        reason TEXT,
        component_count INTEGER DEFAULT 0,
        queue_item_count INTEGER DEFAULT 0,
        state_hash TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_persistence_log_saved ON persistence_log(saved_at DESC);

    CREATE TABLE IF NOT EXISTS manual_override_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT NOT NULL,
        username TEXT,
        role TEXT,
        component_type TEXT NOT NULL,
        component_name TEXT NOT NULL,
        zone_parent TEXT,
        action TEXT NOT NULL,
        previous_state TEXT,
        result TEXT NOT NULL,
        detail TEXT,
        state_after TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_manual_override_created
        ON manual_override_log(created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_manual_override_component
        ON manual_override_log(component_type, component_name, id DESC);

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
    """)
    # Concurrency-oriented SQLite settings for airport traffic.
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA busy_timeout=30000")

    # Safe migration for existing PARKMIND databases.
    event_cols = {row[1] for row in conn.execute("PRAGMA table_info(events)").fetchall()}
    event_required_cols = {
        "received_at": "TEXT",
        "request_fingerprint": "TEXT",
        "processing_status": "TEXT DEFAULT 'RECEIVED'",
        "processed_at": "TEXT",
        "processing_error": "TEXT",
    }
    for col, ddl in event_required_cols.items():
        if col not in event_cols:
            conn.execute(f"ALTER TABLE events ADD COLUMN {col} {ddl}")

    security_cols = {row[1] for row in conn.execute("PRAGMA table_info(network_security_log)").fetchall()}
    if "payload_full" not in security_cols:
        conn.execute("ALTER TABLE network_security_log ADD COLUMN payload_full TEXT")

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

    alert_cols = {row[1] for row in conn.execute("PRAGMA table_info(alerts)").fetchall()}
    if "stay_seconds" not in alert_cols:
        conn.execute("ALTER TABLE alerts ADD COLUMN stay_seconds INTEGER DEFAULT 0")
    if "exit_arrival_time" not in alert_cols:
        conn.execute("ALTER TABLE alerts ADD COLUMN exit_arrival_time TEXT")
    if "stuck_seconds" not in alert_cols:
        conn.execute("ALTER TABLE alerts ADD COLUMN stuck_seconds INTEGER DEFAULT 0")

    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_processing_status ON events(processing_status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_sequence_id ON events(sequence_id)")
    # Airport-scale dashboard/security hot paths. These are safe on old databases
    # and avoid full-table scans as event/car history grows into the thousands.
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cars_status_entry ON cars(status, entry_time DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cars_actual_spot ON cars(actual_spot)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cars_assigned_spot ON cars(assigned_spot)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_server_time ON events(server_time DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_class ON events(event_class)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_alerts_created ON alerts(created_at DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_decisions_plate ON decisions(plate, id DESC)")

    conn.commit()
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


def log_manual_override(component_type, component_name, action, previous_state, result, detail, state_after=""):
    """Durable audit trail for every Level-3 GUI manual-control command."""
    username = str(session.get("user") or "system")
    role = str(session.get("role") or "SYSTEM")
    zone_parent = _component_zone(component_name) if component_name else ""
    conn = db()
    try:
        conn.execute(
            """INSERT INTO manual_override_log(
                   created_at,username,role,component_type,component_name,zone_parent,
                   action,previous_state,result,detail,state_after
               ) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
            (
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"), username, role,
                str(component_type or ""), str(component_name or ""), zone_parent,
                str(action or "").upper(), str(previous_state or ""),
                str(result or ""), str(detail or ""), str(state_after or "")
            )
        )
        conn.commit()
    finally:
        conn.close()


def checkpoint_manual_override(reason):
    """Manual commands are low-rate: persist them immediately instead of waiting for debounce."""
    try:
        persist_runtime_state(f"manual-control:{reason}", write_history=True)
        return True
    except Exception as exc:
        try:
            log_decision("", "MANUAL_PERSIST_FAIL", str(exc), reason)
        except Exception:
            print("[MANUAL PERSIST FAIL]", reason, exc)
        return False


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


def log_penalty(reason, fine_amount, payload=None):
    conn = db()
    conn.execute(
        "INSERT INTO penalties(detected_at, reason, fine_amount, payload) VALUES(?,?,?,?)",
        (
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            str(reason or ""),
            float(fine_amount or 0),
            json.dumps(payload or {})
        )
    )
    conn.commit()
    conn.close()
    with state_lock:
        stats["total_penalties"] += 1
    print(f"[PENALTY] {reason} | fine={fine_amount}")


def request_fingerprint(raw_body):
    return hashlib.sha256(raw_body or b"").hexdigest()


def log_network_security(category, data=None, detail="", fingerprint="", remote_addr="", raw_preview=""):
    data = data if isinstance(data, dict) else {}
    preview = raw_preview
    if not preview:
        try:
            preview = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
        except Exception:
            preview = str(data)
    # Full hostile/duplicate payload is also retained in SQLite for forensic
    # review; the admin page continues showing only a safe short preview.
    try:
        full_payload = json.dumps(data, ensure_ascii=False, separators=(",", ":")) if data else str(raw_preview or "")
    except Exception:
        full_payload = str(data or raw_preview or "")
    preview = str(preview)[:SECURITY_PAYLOAD_PREVIEW]
    conn = db()
    cur = conn.execute(
        """INSERT INTO network_security_log(
               received_at, category, event_id, sequence_id, event_class,
               remote_addr, request_fingerprint, detail, payload_preview, payload_full
           ) VALUES(?,?,?,?,?,?,?,?,?,?)""",
        (
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            str(category or "UNKNOWN"),
            str(data.get("EventId") or ""),
            data.get("SequenceId") if isinstance(data.get("SequenceId"), int) else None,
            str(data.get("EventClass") or ""),
            str(remote_addr or ""),
            str(fingerprint or ""),
            str(detail or ""),
            preview,
            full_payload,
        )
    )
    # Bound hostile/duplicate log growth without depending on AUTOINCREMENT ids.
    # A cheap 1% sampling keeps cleanup amortised and still caps long-running logs.
    if SECURITY_LOG_MAX_ROWS > 0 and random.random() < 0.01:
        conn.execute(
            "DELETE FROM network_security_log WHERE id NOT IN "
            "(SELECT id FROM network_security_log ORDER BY id DESC LIMIT ?)",
            (SECURITY_LOG_MAX_ROWS,)
        )
    conn.commit()
    conn.close()


def canonical_event_key(data, fingerprint=""):
    """Use simulator EventId when supplied; otherwise derive a stable idempotency key."""
    event_id = str(data.get("EventId") or "").strip()
    if event_id:
        return event_id
    seed = "|".join([
        str(data.get("SequenceId") or ""),
        str(data.get("EventClass") or ""),
        str(data.get("ServerDateTime") or ""),
        str(fingerprint or ""),
    ])
    return "synthetic:" + hashlib.sha256(seed.encode("utf-8")).hexdigest()


def save_event(data, verified=False, fingerprint=""):
    """Persist before processing. Returns (inserted, event_key, duplicate_reason)."""
    event_id = canonical_event_key(data, fingerprint)
    seq = data.get("SequenceId")
    conn = db()
    try:
        # Sequence replay check is scoped to the same simulator timestamp so a
        # new simulator run may safely restart its SequenceId numbering.
        server_time = data.get("ServerDateTime")
        if isinstance(seq, int) and server_time:
            existing = conn.execute(
                "SELECT event_id FROM events WHERE sequence_id=? AND server_time=? LIMIT 1",
                (seq, server_time)
            ).fetchone()
            if existing and existing["event_id"] != event_id:
                return False, event_id, f"sequence_replay_existing={existing['event_id']}"

        conn.execute(
            """INSERT INTO events(
                   event_id, sequence_id, event_class, server_time,
                   signature_verified, payload, received_at, request_fingerprint,
                   processing_status
               ) VALUES(?,?,?,?,?,?,?,?,?)""",
            (
                event_id,
                seq,
                data.get("EventClass"),
                data.get("ServerDateTime"),
                1 if verified else 0,
                json.dumps(data),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                fingerprint,
                "RECEIVED",
            )
        )
        conn.commit()
        return True, event_id, ""
    except sqlite3.IntegrityError:
        existing = conn.execute(
            "SELECT request_fingerprint FROM events WHERE event_id=? LIMIT 1", (event_id,)
        ).fetchone()
        if existing and existing["request_fingerprint"] and fingerprint and existing["request_fingerprint"] != fingerprint:
            return False, event_id, "event_id_payload_mismatch"
        return False, event_id, "event_id_duplicate"
    finally:
        conn.close()


def _json_hash(value):
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest(), raw


def _snapshot_controller_state():
    """Return a JSON-safe copy of every meaningful in-memory controller object."""
    with state_lock:
        snapshot = {
            "version": "PARKMIND_LEVEL3_FULL_DATABASE_V1",
            "saved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "components": {
                "spots": {k: dict(v) for k, v in spots.items()},
                "parking_nodes": {k: dict(v) for k, v in parking_nodes.items()},
                "entry_spots": {k: dict(v) for k, v in entry_spots.items()},
                "exit_spots": {k: dict(v) for k, v in exit_spots.items()},
                "leave_spots": {k: dict(v) for k, v in leave_spots.items()},
                "gates": {k: dict(v) for k, v in gates.items()},
                "zones": {k: dict(v) for k, v in zones.items()},
                "lights": {k: dict(v) for k, v in lights.items()},
                "fans": {k: dict(v) for k, v in fans.items()},
                "alarms": {k: dict(v) for k, v in alarms.items()},
            },
            "topology": {
                "ENTRY_GATE": ENTRY_GATE,
                "EXIT_GATE": EXIT_GATE,
                "perimeter_gates": list(perimeter_gates),
                "zone_gates": {k: list(v) for k, v in zone_gates.items()},
                "gate_topology_roles": {k: sorted(list(v)) for k, v in gate_topology_roles.items()},
            },
            "operator": {
                "manual_overrides": json.loads(json.dumps(manual_overrides, default=str)),
                "maintenance_requested": sorted(list(maintenance_requested)),
                "manual_gate_closed": sorted(list(manual_gate_closed)),
                "sensor_quarantined_spots": json.loads(json.dumps(sensor_quarantined_spots, default=str)),
                "suspicious_payment_retries": dict(suspicious_payment_retries),
                "demo_state": json.loads(json.dumps(demo_state, default=str)),
            },
            "routing": {
                "zone_entry_active": dict(zone_entry_active),
                "exit_route_zone": dict(exit_route_zone),
                "reserved_spots": json.loads(json.dumps(reserved_spots, default=str)),
                "entry_queue": json.loads(json.dumps(entry_queue, default=str)),
                "entry_active": json.loads(json.dumps(entry_active, default=str)) if entry_active is not None else None,
                "in_transit": json.loads(json.dumps(in_transit, default=str)),
                "exit_active": json.loads(json.dumps(exit_active, default=str)) if exit_active is not None else None,
                "exit_lane_plate": exit_lane_plate,
                "api_exit_unknown_count": api_exit_unknown_count,
                "escape_corridor_plate": escape_corridor_plate,
                "api_escape_unknown_count": api_escape_unknown_count,
                "to_exit_queue": list(to_exit_queue),
                "physical_exit_queue": list(physical_exit_queue),
                "exit_spot_by_plate": dict(exit_spot_by_plate),
                "charge_attempt_counts": dict(charge_attempt_counts),
                "charge_attempt_scheduled": sorted(list(charge_attempt_scheduled)),
                "leave_finalize_inflight": sorted(list(leave_finalize_inflight)),
            },
            "timing": {
                "last_simulator_time": last_simulator_time,
                # monotonic() itself is process-local; only presence is persisted.
                "last_simulator_time_seen": bool(last_simulator_time_seen_at is not None),
                "last_sync_at": last_sync_at,
            },
            "stats": dict(stats),
            "event_pipeline": {
                "last_sequence_id": last_sequence_id,
                "last_processed_sequence": last_processed_sequence,
                "event_enqueue_counter": event_enqueue_counter,
                "queue_depth": event_queue.qsize(),
            },
        }
    return snapshot


def _component_rows_from_snapshot(snapshot):
    c = snapshot.get("components", {})
    collections = [
        ("parking_spot", c.get("spots", {})),
        ("parking_node", c.get("parking_nodes", {})),
        ("entry_spot", c.get("entry_spots", {})),
        ("exit_spot", c.get("exit_spots", {})),
        ("leave_spot", c.get("leave_spots", {})),
        ("gate", c.get("gates", {})),
        ("zone", c.get("zones", {})),
        ("light", c.get("lights", {})),
        ("fan", c.get("fans", {})),
        ("alarm", c.get("alarms", {})),
    ]
    rows = []
    for component_type, items in collections:
        for name, item in items.items():
            item = dict(item or {})
            h, raw = _json_hash(item)
            broken = 1 if bool(item.get("broken")) else 0
            maint = 1 if bool(item.get("isUnderMaintenance")) else 0
            occupied = 1 if bool(item.get("occupied")) or detected_count(item.get("detectedCars")) > 0 else 0
            # Zones/alarms are informational rather than allocatable hardware.
            available = 0 if (broken or maint) else 1
            rows.append((
                component_type, str(name), str(item.get("zoneParent") or ""),
                broken, maint, available, occupied, h, raw
            ))
    return rows


def _persist_runtime_state_impl(reason="checkpoint", write_history=False):
    """Persist the complete controller cache and normalized component/queue state.

    This is deliberately one SQLite transaction so dashboards never observe a
    half-written checkpoint. Component history is append-only on actual changes.
    """
    snapshot = _snapshot_controller_state()
    state_hash, state_raw = _json_hash(snapshot)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    component_rows = _component_rows_from_snapshot(snapshot)
    queues = snapshot.get("routing", {})
    queue_groups = {
        "entry_queue": queues.get("entry_queue", []),
        "to_exit_queue": queues.get("to_exit_queue", []),
        "physical_exit_queue": queues.get("physical_exit_queue", []),
    }

    conn = db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        old = conn.execute(
            "SELECT state_hash FROM runtime_state WHERE state_key='controller_runtime'"
        ).fetchone()
        old_hash = old["state_hash"] if old else None

        conn.execute(
            """INSERT INTO runtime_state(state_key,state_json,state_hash,updated_at,reason)
               VALUES('controller_runtime',?,?,?,?)
               ON CONFLICT(state_key) DO UPDATE SET
                   state_json=excluded.state_json,
                   state_hash=excluded.state_hash,
                   updated_at=excluded.updated_at,
                   reason=excluded.reason""",
            (state_raw, state_hash, now, str(reason or "checkpoint")[:250])
        )

        if write_history:
            # History stores the operational state without duplicating the full
            # component inventory on every car event. Component changes have
            # their own normalized history table; current full state remains in
            # runtime_state for restart recovery.
            history_snapshot = dict(snapshot)
            history_snapshot.pop("components", None)
            _, history_raw = _json_hash(history_snapshot)
            conn.execute(
                "INSERT INTO runtime_state_history(created_at,reason,state_hash,state_json) VALUES(?,?,?,?)",
                (now, str(reason or "checkpoint")[:250], state_hash, history_raw)
            )

        existing_hashes = {
            (r["component_type"], r["name"]): r["state_hash"]
            for r in conn.execute("SELECT component_type,name,state_hash FROM component_state").fetchall()
        }
        current_keys = set()
        for row in component_rows:
            component_type, name, zone_parent, broken, maint, available, occupied, h, raw = row
            current_keys.add((component_type, name))
            if existing_hashes.get((component_type, name)) != h:
                conn.execute(
                    """INSERT INTO component_state_history(
                           changed_at,component_type,name,zone_parent,state_hash,state_json
                       ) VALUES(?,?,?,?,?,?)""",
                    (now, component_type, name, zone_parent, h, raw)
                )
            conn.execute(
                """INSERT INTO component_state(
                       component_type,name,zone_parent,broken,under_maintenance,
                       available,occupied,state_hash,state_json,updated_at
                   ) VALUES(?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(component_type,name) DO UPDATE SET
                       zone_parent=excluded.zone_parent,
                       broken=excluded.broken,
                       under_maintenance=excluded.under_maintenance,
                       available=excluded.available,
                       occupied=excluded.occupied,
                       state_hash=excluded.state_hash,
                       state_json=excluded.state_json,
                       updated_at=excluded.updated_at""",
                (component_type, name, zone_parent, broken, maint, available, occupied, h, raw, now)
            )

        # Remove stale current-state rows when a level layout changes. History remains.
        if current_keys:
            db_keys = conn.execute("SELECT component_type,name FROM component_state").fetchall()
            for r in db_keys:
                key = (r["component_type"], r["name"])
                if key not in current_keys:
                    conn.execute(
                        "DELETE FROM component_state WHERE component_type=? AND name=?", key
                    )

        conn.execute("DELETE FROM controller_queue_state")
        queue_count = 0
        for queue_name, items in queue_groups.items():
            for position, item in enumerate(items):
                conn.execute(
                    "INSERT INTO controller_queue_state(queue_name,position,item_json,updated_at) VALUES(?,?,?,?)",
                    (queue_name, position, json.dumps(item, ensure_ascii=False, default=str), now)
                )
                queue_count += 1

        conn.execute(
            "INSERT INTO persistence_log(saved_at,reason,component_count,queue_item_count,state_hash) VALUES(?,?,?,?,?)",
            (now, str(reason or "checkpoint")[:250], len(component_rows), queue_count, state_hash)
        )

        if RUNTIME_HISTORY_MAX_ROWS > 0:
            conn.execute(
                "DELETE FROM runtime_state_history WHERE id NOT IN "
                "(SELECT id FROM runtime_state_history ORDER BY id DESC LIMIT ?)",
                (RUNTIME_HISTORY_MAX_ROWS,)
            )
        if COMPONENT_HISTORY_MAX_ROWS > 0:
            conn.execute(
                "DELETE FROM component_state_history WHERE id NOT IN "
                "(SELECT id FROM component_state_history ORDER BY id DESC LIMIT ?)",
                (COMPONENT_HISTORY_MAX_ROWS,)
            )
        # Keep persistence audit bounded while retaining a useful recent trail.
        conn.execute(
            "DELETE FROM persistence_log WHERE id NOT IN "
            "(SELECT id FROM persistence_log ORDER BY id DESC LIMIT 5000)"
        )
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()



def persist_runtime_state(reason="checkpoint", write_history=False):
    """Serialized wrapper around the expensive full-state checkpoint.

    The implementation still writes one atomic SQLite snapshot, but only one caller
    can build/write it at a time. Any successful full checkpoint also resets the
    event-burst debounce counters.
    """
    global last_runtime_persist_monotonic, events_since_runtime_persist
    with runtime_persist_lock:
        ok = _persist_runtime_state_impl(reason, write_history=write_history)
        if ok:
            with runtime_persist_meta_lock:
                last_runtime_persist_monotonic = time.monotonic()
                events_since_runtime_persist = 0
        return ok


def maybe_persist_after_event(event_id):
    """Coalesce expensive full checkpoints during event bursts.

    Raw webhook payloads are already durable in ``events`` before they reach the
    worker. Normal car/payment/anomaly/audit writes performed by ``handle_event``
    are also committed by their existing DB functions. This debounce affects only
    the heavyweight whole-controller/component recovery snapshot.
    """
    global events_since_runtime_persist

    now = time.monotonic()
    with runtime_persist_meta_lock:
        events_since_runtime_persist += 1
        pending = events_since_runtime_persist
        elapsed = (
            float("inf")
            if not last_runtime_persist_monotonic
            else now - last_runtime_persist_monotonic
        )
        due = (
            elapsed >= max(0.01, RUNTIME_EVENT_PERSIST_DEBOUNCE_SEC)
            or pending >= max(1, RUNTIME_EVENT_PERSIST_MAX_EVENTS)
        )

    if not due:
        return False

    try:
        persist_runtime_state(f"event-batch:{event_id}", write_history=True)
        return True
    except Exception as pe:
        # Persistence must never convert an otherwise-successful event into ERROR.
        # The dirty in-memory state remains available and the periodic watchdog will
        # retry a complete checkpoint on its next pass.
        try:
            log_decision(
                "",
                "PERSIST_FAIL",
                f"event={event_id}: {pe}",
                "Event state already applied; periodic checkpoint will retry full persistence.",
            )
        except Exception:
            print(f"[DB PERSIST ERROR] event={event_id}: {pe}")
        return False


def restore_runtime_state_from_db():
    """Restore cached state after a controller restart before normal live syncing."""
    global ENTRY_GATE, EXIT_GATE, perimeter_gates, zone_gates, gate_topology_roles
    global entry_active, exit_active, exit_lane_plate, escape_corridor_plate
    global api_exit_unknown_count, api_escape_unknown_count
    global last_simulator_time, last_simulator_time_seen_at, last_sync_at
    global last_sequence_id, last_processed_sequence, event_enqueue_counter

    conn = db()
    row = conn.execute(
        "SELECT state_json,updated_at FROM runtime_state WHERE state_key='controller_runtime'"
    ).fetchone()
    conn.close()
    if not row or not row["state_json"]:
        return False
    try:
        saved = json.loads(row["state_json"])
    except Exception as exc:
        print("[DB RESTORE] Invalid persisted runtime state:", exc)
        return False

    c = saved.get("components", {})
    t = saved.get("topology", {})
    op = saved.get("operator", {})
    r = saved.get("routing", {})
    timing = saved.get("timing", {})
    ep = saved.get("event_pipeline", {})

    with state_lock:
        for target, key in [
            (spots, "spots"), (parking_nodes, "parking_nodes"),
            (entry_spots, "entry_spots"), (exit_spots, "exit_spots"),
            (leave_spots, "leave_spots"), (gates, "gates"),
            (zones, "zones"), (lights, "lights"), (fans, "fans"),
            (alarms, "alarms"),
        ]:
            target.clear()
            target.update(c.get(key, {}) or {})

        ENTRY_GATE = t.get("ENTRY_GATE")
        EXIT_GATE = t.get("EXIT_GATE")
        perimeter_gates = list(t.get("perimeter_gates", []) or [])
        zone_gates = defaultdict(list, {k: list(v) for k, v in (t.get("zone_gates", {}) or {}).items()})
        gate_topology_roles = defaultdict(set, {
            k: set(v) for k, v in (t.get("gate_topology_roles", {}) or {}).items()
        })

        manual_overrides.clear()
        manual_overrides.update(op.get("manual_overrides", {}) or {})
        for required in ("gate", "fan", "light", "light_group"):
            manual_overrides.setdefault(required, {})
        maintenance_requested.clear()
        maintenance_requested.update(op.get("maintenance_requested", []) or [])
        manual_gate_closed.clear()
        manual_gate_closed.update(op.get("manual_gate_closed", []) or [])
        demo_state.clear()
        demo_state.update(op.get("demo_state", {}) or {})
        sensor_quarantined_spots.clear()
        sensor_quarantined_spots.update(op.get("sensor_quarantined_spots", {}) or {})
        suspicious_payment_retries.clear()
        suspicious_payment_retries.update(op.get("suspicious_payment_retries", {}) or {})

        zone_entry_active.clear(); zone_entry_active.update(r.get("zone_entry_active", {}) or {})
        exit_route_zone.clear(); exit_route_zone.update(r.get("exit_route_zone", {}) or {})
        reserved_spots.clear(); reserved_spots.update(r.get("reserved_spots", {}) or {})
        entry_queue[:] = list(r.get("entry_queue", []) or [])
        entry_active = r.get("entry_active")
        in_transit.clear(); in_transit.update(r.get("in_transit", {}) or {})
        exit_active = r.get("exit_active")
        exit_lane_plate = r.get("exit_lane_plate")
        api_exit_unknown_count = int(r.get("api_exit_unknown_count") or 0)
        escape_corridor_plate = r.get("escape_corridor_plate")
        api_escape_unknown_count = int(r.get("api_escape_unknown_count") or 0)
        to_exit_queue[:] = list(r.get("to_exit_queue", []) or [])
        physical_exit_queue[:] = list(r.get("physical_exit_queue", []) or [])
        exit_spot_by_plate.clear(); exit_spot_by_plate.update(r.get("exit_spot_by_plate", {}) or {})
        charge_attempt_counts.clear(); charge_attempt_counts.update(r.get("charge_attempt_counts", {}) or {})
        charge_attempt_scheduled.clear(); charge_attempt_scheduled.update(r.get("charge_attempt_scheduled", []) or [])
        leave_finalize_inflight.clear(); leave_finalize_inflight.update(r.get("leave_finalize_inflight", []) or [])

        stats.update(saved.get("stats", {}) or {})
        last_simulator_time = timing.get("last_simulator_time")
        last_simulator_time_seen_at = time.monotonic() if last_simulator_time else None
        last_sync_at = timing.get("last_sync_at")
        last_sequence_id = ep.get("last_sequence_id")
        last_processed_sequence = ep.get("last_processed_sequence")
        event_enqueue_counter = int(ep.get("event_enqueue_counter") or 0)

    print(f"[DB RESTORE] Restored complete controller state saved at {row['updated_at']}")
    return True


def runtime_persistence_watchdog():
    """Catch timer/manual/UI mutations that are not directly caused by a webhook."""
    while True:
        try:
            persist_runtime_state("periodic checkpoint", write_history=False)
        except Exception as exc:
            print("[DB PERSIST ERROR]", exc)
        time.sleep(max(0.25, RUNTIME_PERSIST_INTERVAL_SEC))


def mark_event_status(event_id, status, error=""):
    conn = db()
    processed_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S") if status in ("PROCESSED", "ERROR") else None
    conn.execute(
        "UPDATE events SET processing_status=?, processed_at=COALESCE(?, processed_at), processing_error=? WHERE event_id=?",
        (status, processed_at, str(error or "")[:1000], event_id)
    )
    conn.commit()
    conn.close()


def enqueue_persisted_event(event_id, sequence_id=None):
    global event_enqueue_counter
    with event_enqueue_lock:
        event_enqueue_counter += 1
        counter = event_enqueue_counter
    # No SequenceId sorts after normal simulator events but remains FIFO.
    priority = int(sequence_id) if isinstance(sequence_id, int) else (10**18 + counter)
    try:
        event_queue.put_nowait((priority, counter, event_id))
        mark_event_status(event_id, "QUEUED")
        return True
    except queue.Full:
        mark_event_status(event_id, "DB_PENDING", "in-memory queue full; durable DB refill will retry")
        return False


def event_worker():
    """Single ordered worker: bounded concurrency is deliberate for state-machine correctness."""
    global last_processed_sequence
    while True:
        try:
            # Small batching window lets simultaneous web requests enter the priority queue
            # before the next state transition is selected.
            time.sleep(EVENT_BATCH_DELAY_SEC)
            priority, counter, event_id = event_queue.get()
            conn = db()
            row = conn.execute(
                "SELECT event_id, sequence_id, payload, processing_status FROM events WHERE event_id=?",
                (event_id,)
            ).fetchone()
            conn.close()
            if not row:
                event_queue.task_done()
                continue
            if row["processing_status"] == "PROCESSED":
                event_queue.task_done()
                continue

            try:
                data = json.loads(row["payload"] or "{}")
                mark_event_status(event_id, "PROCESSING")
                check_sequence_gap(row["sequence_id"])
                handle_event(data)
                # Raw event data is already durable before queueing. Heavy full-state
                # snapshots are debounced during bursts so thousands of simultaneous
                # events do not cause thousands of component-table checkpoints.
                # Persistence failure is isolated inside maybe_persist_after_event().
                maybe_persist_after_event(event_id)
                seq = row["sequence_id"]
                if isinstance(seq, int):
                    last_processed_sequence = max(last_processed_sequence or seq, seq)
                mark_event_status(event_id, "PROCESSED")
            except Exception as exc:
                mark_event_status(event_id, "ERROR", exc)
                log_decision("", "EVENT_WORKER_ERROR", f"{event_id}: {exc}", "Durable event remains in audit DB.")
            finally:
                event_queue.task_done()
        except Exception as exc:
            print("[EVENT WORKER ERROR]", exc)
            time.sleep(0.25)


def event_db_refill_watchdog():
    """Recover DB-persisted events after queue overflow or process restart."""
    while True:
        try:
            if event_queue.qsize() < max(1, EVENT_QUEUE_MAX // 2):
                conn = db()
                rows = conn.execute(
                    """SELECT event_id, sequence_id FROM events
                       WHERE COALESCE(processing_status,'RECEIVED') IN ('RECEIVED','DB_PENDING')
                       ORDER BY CASE WHEN sequence_id IS NULL THEN 1 ELSE 0 END, sequence_id, id
                       LIMIT 200"""
                ).fetchall()
                conn.close()
                for row in rows:
                    if not enqueue_persisted_event(row["event_id"], row["sequence_id"]):
                        break
        except Exception as exc:
            print("[EVENT REFILL ERROR]", exc)
        time.sleep(EVENT_DB_REFILL_SEC)


def recover_pending_events_on_startup():
    """A clean restart must not lose events accepted just before a crash."""
    conn = db()
    conn.execute(
        "UPDATE events SET processing_status='RECEIVED', processing_error='recovered after restart' "
        "WHERE COALESCE(processing_status,'RECEIVED') IN ('PROCESSING','QUEUED','DB_PENDING')"
    )
    conn.commit()
    conn.close()


def ensure_event_workers_started():
    """Idempotent startup for direct Python launch and WSGI-style serving."""
    global event_worker_started
    if event_worker_started:
        return
    with event_worker_start_lock:
        if event_worker_started:
            return
        init_db()
        restore_runtime_state_from_db()
        recover_pending_events_on_startup()
        threading.Thread(target=event_worker, daemon=True, name="parkmind-event-worker").start()
        threading.Thread(target=event_db_refill_watchdog, daemon=True, name="parkmind-event-refill").start()
        threading.Thread(target=runtime_persistence_watchdog, daemon=True, name="parkmind-db-persistence").start()
        event_worker_started = True


def upsert_car(plate, **fields):
    # Never allow malformed webhooks/recovery paths to create NULL/blank ghost cars.
    if not isinstance(plate, str) or not plate.strip():
        raise ValueError("upsert_car called without a valid plate")
    plate = plate.strip()
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
      True  -> a non-empty signature was present and matched a supported canonical form.
      False -> a non-empty signature was present but DID NOT match.
      None  -> simulator sent no usable signature (e.g. Signature=None).

    The simulator may canonicalise scalars using JSON rules, so PARKMIND accepts
    both the original pipe-joined-value MD5 and compact sorted-JSON MD5. Unsigned
    simulator events remain accepted for backwards compatibility.
    """
    raw_sig = data.get("Signature")
    if raw_sig is None or str(raw_sig).strip().lower() in ("", "none", "null"):
        return None

    received_sig = str(raw_sig).strip().lower()
    payload = {k: v for k, v in data.items() if k != "Signature"}

    def canon_pipe(v):
        if v is None:
            return ""
        if isinstance(v, bool):
            return "true" if v else "false"
        if isinstance(v, float):
            return format(v, "g")
        return str(v)

    joined = "|".join(canon_pipe(payload[k]) for k in sorted(payload))
    pipe_md5 = hashlib.md5(joined.encode("utf-8")).hexdigest()
    json_md5 = hashlib.md5(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    ).hexdigest()

    ok = (
        hmac.compare_digest(pipe_md5, received_sig)
        or hmac.compare_digest(json_md5, received_sig)
    )
    if not ok:
        log_decision(
            "", "SIGNATURE_SCHEME_MISMATCH",
            f"pipe={pipe_md5} json={json_md5} got={received_sig[:12]}…",
            "Supplied signature matched neither supported canonical form."
        )
    return ok


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
                r = get_http_session().post(
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


class SimulatorClientError(Exception):
    """Non-retryable 4xx response from the parking simulator."""
    def __init__(self, status_code, body, method="", path=""):
        self.status_code = int(status_code)
        self.body = str(body or "")
        self.method = str(method or "")
        self.path = str(path or "")
        super().__init__(f"{self.status_code}: {self.body[:200]}")


def sim_request(method, path, **kwargs):
    global token
    if not token:
        sim_login()

    headers = kwargs.pop("headers", {})
    headers["Authorization"] = f"Bearer {token}"

    for attempt in range(3):
        try:
            r = get_http_session().request(method, f"{SIM_BASE}{path}", headers=headers, timeout=8, **kwargs)
            if r.status_code == 401:
                sim_login()
                headers["Authorization"] = f"Bearer {token}"
                continue
            if r.status_code >= 500:
                wait = 2 ** attempt
                if VERBOSE_WEBHOOK_LOG:
                    print(f"[API 5xx] {method} {path} -> {r.status_code}; retry in {wait}s")
                time.sleep(wait)
                continue
            if 400 <= r.status_code < 500:
                body = r.text[:1000]
                if VERBOSE_WEBHOOK_LOG:
                    print(f"[API 4xx] {method} {path} -> {r.status_code} {body[:200]}")
                raise SimulatorClientError(r.status_code, body, method, path)
            return r
        except SimulatorClientError:
            raise
        except requests.exceptions.RequestException as e:
            wait = 2 ** attempt
            if VERBOSE_WEBHOOK_LOG:
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
    """Build Level-3 gate topology only from /list-barriers.

    The simulator exposes barrier ``name`` + ``zoneParent`` but no reliable
    entry/exit role.  PARKMIND therefore keeps the topology API-driven:
      - perimeter_gates: zoneParent == ""
      - zone_gates[ZONE]: barriers returned for that zone

    ENTRY_GATE and EXIT_GATE are compatibility aliases only.  Real routing uses
    healthy alternatives from each group and can fail over when one gate breaks.
    """
    global ENTRY_GATE, EXIT_GATE, perimeter_gates, zone_gates, gate_topology_roles

    names = [str(g.get("name")) for g in barrier_data if g.get("name")]
    if not names:
        raise RuntimeError("Simulator returned no barriers from /list-barriers")

    perimeter = []
    by_zone = defaultdict(list)
    duplicate_roles = defaultdict(set)
    gate_topology_roles = defaultdict(set)

    for g in barrier_data:
        name = str(g.get("name") or "").strip()
        if not name:
            continue
        zone = str(g.get("zoneParent") or "").strip()
        role = zone or "PERIMETER"
        duplicate_roles[name].add(role)
        gate_topology_roles[name].add(role)
        if zone:
            if name not in by_zone[zone]:
                by_zone[zone].append(name)
        else:
            if name not in perimeter:
                perimeter.append(name)

    perimeter_gates = list(perimeter)
    zone_gates = defaultdict(list, {z: list(v) for z, v in by_zone.items()})

    # Prefer a currently usable perimeter gate, but never hardcode a literal
    # name. If all are unavailable the aliases remain deterministic so the
    # dashboard still displays the discovered topology; route viability checks
    # will safely hold cars instead of waiting forever on a failed barrier.
    usable = [n for n in perimeter_gates if gate_route_available(n)]
    pool = usable or perimeter_gates or names
    ENTRY_GATE = pool[0] if pool else None

    distinct = [n for n in pool if n != ENTRY_GATE]
    EXIT_GATE = distinct[-1] if distinct else ENTRY_GATE

    ambiguous = {n: sorted(v) for n, v in duplicate_roles.items() if len(v) > 1}
    if ambiguous:
        log_decision(
            "", "GATE_NAME_ROLE_WARNING", str(ambiguous),
            "Same barrier name appeared in more than one topology role; routing keeps the identifier but de-duplicates commands."
        )

    log_decision(
        "", "GATE_TOPOLOGY",
        f"perimeter={perimeter_gates}; zone_gates={dict(zone_gates)}; aliases=({ENTRY_GATE},{EXIT_GATE})",
        "Level-3 topology is API-derived; aliases are healthy preferences, not hardcoded routing dependencies."
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

    # Keep plate->ExitSpot recovery mappings bounded across long-running/restarted
    # airport sessions. Only active trips are eligible to retain a mapping.
    conn = db()
    try:
        active_exit_mapping_plates = {
            row["plate"] for row in conn.execute(
                "SELECT plate FROM cars WHERE plate IS NOT NULL AND TRIM(plate)<>'' "
                "AND status NOT IN ('LEFT','ESCAPED_UNPAID')"
            ).fetchall()
        }
    finally:
        conn.close()

    with state_lock:
        for stale_plate in list(exit_spot_by_plate.keys()):
            if stale_plate not in active_exit_mapping_plates:
                exit_spot_by_plate.pop(stale_plate, None)

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
                detected = detected_count(item.get("detectedCars"))
                impossible_sensor_count = detected > 1
                locally_quarantined = name in sensor_quarantined_spots
                spots[name] = {
                    **item,
                    "occupied": detected > 0,
                    "usage_count": prev_spots.get(name, {}).get("usage_count", 0),
                    "sensor_abnormality": impossible_sensor_count or locally_quarantined,
                    "isUnderMaintenance": bool(item.get("isUnderMaintenance")) or impossible_sensor_count or locally_quarantined,
                }
                if impossible_sensor_count and name not in sensor_quarantined_spots:
                    token = f"sync-{time.time_ns()}"
                    sensor_quarantined_spots[name] = {
                        "reason": f"API reported detectedCars={detected} for one bay",
                        "since": time.time(),
                        "token": token,
                        "preexisting_maintenance": bool(item.get("isUnderMaintenance")),
                    }
                    timer = threading.Timer(
                        SENSOR_QUARANTINE_SEC, _clear_sensor_quarantine, args=(name, token)
                    )
                    timer.daemon = True
                    timer.start()
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
    # API discovery itself is data: persist the complete live component inventory.
    persist_runtime_state("simulator sync", write_history=True)


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



def gate_route_available(name):
    """True only when a barrier is usable by automatic Level-3 routing."""
    if not name or name not in gates:
        return False
    if not gate_safe(name):
        return False
    # A deliberate MANUAL CLOSED override is treated exactly like an unavailable
    # gate for route selection. Other healthy gates may still be used.
    if gate_manual_override(name) is False:
        return False
    return True


def available_route_gates(names):
    """De-duplicated healthy alternatives from an API-discovered gate group."""
    result = []
    for name in names or []:
        if name in result:
            continue
        if gate_route_available(name):
            result.append(name)
    return result


def select_route_gate(names, preferred=None):
    """Pick one healthy alternative without hardcoding names.

    Preference order keeps existing behavior stable: preferred alias first when
    healthy, then already-open barriers, then highest component health, then the
    least-cycled / natural name.  Only ONE alternative from a gate group becomes
    a hard movement requirement, so one failed sibling cannot deadlock the car.
    """
    candidates = available_route_gates(names)
    if not candidates:
        return None
    if preferred in candidates and len(gate_topology_roles.get(preferred, set())) <= 1:
        return preferred

    def rank(name):
        # A duplicate identifier that appears in multiple topology roles is
        # usable as a last resort, but unambiguous alternatives are safer.
        ambiguous_rank = 1 if len(gate_topology_roles.get(name, set())) > 1 else 0
        state = str(gates.get(name, {}).get("state") or "")
        open_rank = 0 if state in ("Open", "Opening") else 1
        health_rank = -component_health_score(name, "gate")
        uses = int(gates.get(name, {}).get("usage_count") or 0)
        return (ambiguous_rank, open_rank, health_rank, uses, _natural_zone_key(name))

    return sorted(candidates, key=rank)[0]


def refresh_gate_failover_aliases(reason="health change"):
    """Refresh compatibility aliases after a barrier fails/fixes."""
    global ENTRY_GATE, EXIT_GATE
    old_entry, old_exit = ENTRY_GATE, EXIT_GATE
    healthy = available_route_gates(perimeter_gates)
    pool = healthy or list(perimeter_gates)
    if pool:
        preferred_entry = old_entry if gate_route_available(old_entry) else None
        ENTRY_GATE = select_route_gate(pool, preferred=preferred_entry) or pool[0]
        alternatives = [n for n in pool if n != ENTRY_GATE]
        preferred_exit = old_exit if old_exit in alternatives and gate_route_available(old_exit) else None
        EXIT_GATE = select_route_gate(alternatives, preferred=preferred_exit) if alternatives else ENTRY_GATE
        EXIT_GATE = EXIT_GATE or ENTRY_GATE
    if (ENTRY_GATE, EXIT_GATE) != (old_entry, old_exit):
        with state_lock:
            stats["gate_failovers"] += 1
        log_decision(
            "", "GATE_ALIAS_FAILOVER",
            f"entry {old_entry}->{ENTRY_GATE}; exit {old_exit}->{EXIT_GATE}",
            reason,
        )


def route_path_viable(zone_name, perimeter_role=None):
    """Validate that every *group* needed for a movement has an alternative."""
    zone_name = str(zone_name or "")
    if zone_name and zone_gates.get(zone_name) and not available_route_gates(zone_gates.get(zone_name)):
        return False, f"NO_HEALTHY_ZONE_GATE({zone_name})"
    if perimeter_role in ("entry", "exit") and perimeter_gates and not available_route_gates(perimeter_gates):
        return False, f"NO_HEALTHY_PERIMETER_GATE({perimeter_role})"
    return True, "ROUTE_OK"



def _route_gate_names_for_zone(zone_name, perimeter_role=None):
    """Return one healthy alternative from each API-discovered gate group.

    Level 2 required every barrier in a group to become Open.  At airport scale
    that turns one broken gate into a total route deadlock.  Level 3 treats gates
    sharing the same topology role as alternatives and requires one healthy gate
    from the target/source zone plus one healthy perimeter gate when needed.
    """
    names = []
    zone_name = str(zone_name or "")

    if perimeter_role == "entry":
        selected = select_route_gate(perimeter_gates, preferred=ENTRY_GATE)
        if selected:
            names.append(selected)

    zone_selected = select_route_gate(zone_gates.get(zone_name or "", []))
    if zone_selected and zone_selected not in names:
        names.append(zone_selected)

    if perimeter_role == "exit":
        selected = select_route_gate(perimeter_gates, preferred=EXIT_GATE)
        if selected and selected not in names:
            names.append(selected)

    return names



def open_route_gates(zone_name, reason="route", perimeter_role=None, gate_names=None):
    """Open the selected healthy route barriers only."""
    selected = list(gate_names) if gate_names is not None else _route_gate_names_for_zone(
        zone_name, perimeter_role=perimeter_role
    )
    opened = []
    for name in selected:
        try:
            if gate_route_available(name):
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



def _clear_sensor_quarantine(name, token):
    """Release a local sensor quarantine only if it has not been superseded."""
    with state_lock:
        current = sensor_quarantined_spots.get(name)
        if not current or current.get("token") != token:
            return
        item = spots.get(name, {})
        # A real component failure / simulator maintenance remains authoritative.
        if item.get("broken") or name in alarms or name in maintenance_requested:
            return
        preserve_maintenance = bool(current.get("preexisting_maintenance"))
        sensor_quarantined_spots.pop(name, None)
        if name in spots:
            spots[name]["sensor_abnormality"] = False
            spots[name]["isUnderMaintenance"] = preserve_maintenance
    log_decision(
        "", "SENSOR_QUARANTINE_CLEARED", name,
        "Temporary Level-3 sensor quarantine expired with no active broken/alarm state."
    )


def quarantine_spot_for_sensor_abnormality(name, reason, seconds=None):
    """Temporarily remove a parking bay from allocation after sensor abnormality."""
    name = str(name or "")
    if not name or name not in spots:
        return False
    seconds = SENSOR_QUARANTINE_SEC if seconds is None else max(1, int(seconds))
    token = f"sensor-{time.time_ns()}"

    with state_lock:
        first = name not in sensor_quarantined_spots
        preexisting_maintenance = bool(spots[name].get("isUnderMaintenance"))
        sensor_quarantined_spots[name] = {
            "reason": str(reason or "sensor abnormality"),
            "since": time.time(),
            "token": token,
            "preexisting_maintenance": preexisting_maintenance,
        }
        spots[name]["sensor_abnormality"] = True
        spots[name]["isUnderMaintenance"] = True

        # If this bay was merely reserved and the vehicle has not yet been sent,
        # release the reservation so process_entry_queue can choose another bay.
        reservation = reserved_spots.get(name)
        active_plate = entry_active.get("plate") if entry_active else None
        active_spot = entry_active.get("spot") if entry_active else None
        active_sent = bool(entry_active.get("sent")) if entry_active else False
        should_requeue = bool(reservation and active_plate == reservation.get("plate") and active_spot == name and not active_sent)

    if first:
        with state_lock:
            stats["sensor_quarantines"] += 1
        log_anomaly("", "SPOT_SENSOR_ABNORMALITY", f"{name}: {reason}")
    log_decision(
        "", "SPOT_SENSOR_MAINTENANCE", name,
        f"Removed from availability for {seconds}s: {reason}"
    )

    if should_requeue:
        requeue_active_entry_due_to_route_failure(
            f"Sensor abnormality quarantined assigned spot {name}"
        )

    timer = threading.Timer(seconds, _clear_sensor_quarantine, args=(name, token))
    timer.daemon = True
    timer.start()
    return True


def requeue_active_entry_due_to_route_failure(reason):
    """Safely reconsider an entry assignment while the car is still at EntrySpot."""
    global entry_active
    with state_lock:
        active = dict(entry_active or {})
        if not active or active.get("sent"):
            return False
        plate = active.get("plate")
        spot = active.get("spot")
        zone = str(active.get("zone") or "")
        car = get_car(plate) or {}

        reservation = reserved_spots.get(spot)
        if reservation and reservation.get("plate") == plate:
            reserved_spots.pop(spot, None)
        if zone and zone_entry_active.get(zone) == plate:
            zone_entry_active.pop(zone, None)
        entry_active = None
        if not any(x.get("plate") == plate for x in entry_queue):
            entry_queue.insert(0, {
                "plate": plate,
                "car_type": car.get("car_type") or "Normal",
                "planned": int(car.get("planned_minutes") or 0),
                "entry_spot": active.get("entry_spot"),
                "entry_zone": active.get("entry_zone"),
            })
        upsert_car(plate, status="ENTRY_RETRY", decision=str(reason))

    log_decision(
        plate, "LEVEL3_ENTRY_REROUTE", str(reason),
        "Reservation and zone lock released; same vehicle will be assigned through another healthy route."
    )
    timer = threading.Timer(0.05, process_entry_queue)
    timer.daemon = True
    timer.start()
    return True


def recover_from_gate_failure(name):
    """Fail over active movements when one gate becomes unavailable."""
    refresh_gate_failover_aliases(f"gate unavailable: {name}")
    ensure_keep_open_gates()

    # Entry: before dispatch, swap failed route gate for a healthy sibling. If
    # the whole target-zone gate group is down, re-run spot/zone selection.
    with state_lock:
        active_entry = dict(entry_active or {})
    if active_entry and name in list(active_entry.get("required_gates") or []):
        plate = active_entry.get("plate")
        zone = str(active_entry.get("zone") or "")
        if not active_entry.get("sent"):
            viable, why = route_path_viable(zone, perimeter_role="entry")
            if not viable:
                requeue_active_entry_due_to_route_failure(why)
            else:
                required = _route_gate_names_for_zone(zone, perimeter_role="entry")
                with state_lock:
                    if entry_active and entry_active.get("plate") == plate:
                        entry_active["required_gates"] = list(required)
                opened = open_route_gates(zone, reason=f"entry failover after {name}", perimeter_role="entry", gate_names=required)
                with state_lock:
                    stats["gate_failovers"] += 1
                log_decision(
                    plate, "ENTRY_GATE_FAILOVER",
                    f"failed={name}; replacement_route={opened}",
                    "Vehicle stayed at EntrySpot until a healthy alternative route was prepared."
                )
                try_dispatch_entry_when_ready(plate)
        else:
            # The car already received goto; open another sibling so the
            # simulator can continue/reroute without issuing a duplicate trip.
            open_route_gates(zone, reason=f"in-transit entry failover after {name}", perimeter_role="entry")

    # Cars already travelling from their bay toward an ExitSpot can use another
    # healthy zone gate without changing the one-car exit-lane ownership.
    with state_lock:
        outbound = [(p, z) for p, z in exit_route_zone.items() if name in zone_gates.get(str(z or ""), [])]
    for plate, zone in outbound:
        opened = open_route_gates(zone, reason=f"exit-approach failover after {name}", perimeter_role=None)
        log_decision(plate, "EXIT_APPROACH_GATE_FAILOVER", f"failed={name}; opened={opened}")

    # Paid Stage-B release: recompute the required alternative gates and keep the
    # hard payment interlock intact.
    with state_lock:
        active_exit = dict(exit_active or {})
    if active_exit and name in list(active_exit.get("required_gates") or []):
        plate = active_exit.get("plate")
        zone, required = _exit_release_required_gates(plate)
        viable, why = route_path_viable(zone, perimeter_role="exit")
        if viable:
            with state_lock:
                if exit_active and exit_active.get("plate") == plate:
                    exit_active["required_gates"] = list(required)
            opened = open_route_gates(zone, reason=f"paid-exit failover after {name}", perimeter_role="exit", gate_names=required)
            with state_lock:
                stats["gate_failovers"] += 1
            log_decision(plate, "EXIT_GATE_FAILOVER", f"failed={name}; replacement_route={opened}")
            try_release_paid_when_ready(plate)
        else:
            log_decision(plate, "EXIT_GATE_FAILOVER_HOLD", why, "Paid vehicle remains safely held until a gate is restored.")


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
    """Level-3 spawn guard: keep all discovered perimeter alternatives open.

    The simulator can create a vehicle before PARKMIND receives its EntrySpot
    webhook. With multiple airport entrances there is no safe way to know which
    perimeter entrance will be used next, so every healthy AUTO perimeter gate
    is kept open. Payment security is still enforced by the hard software
    payment interlock: unpaid cars never receive the leavepark command.
    """
    names = {n for n in KEEP_OPEN_GATES_ENV if n in gates}
    names.update(n for n in perimeter_gates if n in gates)
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

    # The car must never be sent while any selected barrier is merely Opening or
    # Closed.  Only the simulator's gate_action state == Open authorizes motion.
    if required and not all(gate_fully_open(name) for name in required):
        return False

    # Revalidate the API-discovered route immediately before goto. This catches a
    # failure/maintenance/manual-close event that arrived after assignment but
    # before the car was released.
    if target_zone and zone_gates.get(target_zone):
        selected_zone_gate = any(name in zone_gates.get(target_zone, []) for name in required)
        if not selected_zone_gate:
            log_decision(plate, "ENTRY_ROUTE_INVALID", f"No selected gate for {target_zone}",
                         "goto blocked; route no longer satisfies discovered topology.")
            return False
    if perimeter_gates:
        selected_perimeter = any(name in perimeter_gates for name in required)
        if not selected_perimeter:
            log_decision(plate, "ENTRY_ROUTE_INVALID", "No selected perimeter entry gate",
                         "goto blocked; route no longer satisfies discovered topology.")
            return False
    if any(not gate_route_available(name) for name in required):
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


def _validate_api_car_destination(destination):
    """Validate the simulator's documented goto destination contract.

    The supplied API documents exactly three destination forms:
      * a discovered parking-spot name;
      * ``exit``;
      * ``leavepark``.

    Refusing an unknown destination here prevents a controller bug or malformed
    internal state from producing an unsupported simulator command.
    """
    dest = str(destination or "").strip()
    if dest in ("exit", "leavepark"):
        return dest
    if dest in spots:
        return dest
    raise ValueError(f"Unsupported simulator goto destination: {destination!r}")


def send_car(plate, destination):
    """Send a car using only the documented simulator ``goto`` endpoint."""
    plate = str(plate or "").strip()
    if not plate:
        raise ValueError("Cannot route a car without a plate identifier")
    destination = _validate_api_car_destination(destination)
    # API documentation explicitly allows plate identifiers with or without
    # spaces.  Compacting keeps the existing stable PARKMIND behaviour.
    plate_path = quote(plate.replace(" ", ""), safe="")
    dest_path = quote(destination, safe="")
    sim_request("POST", f"/car/{plate_path}/goto/{dest_path}")
    log_decision(plate, "CAR_GOTO", destination, "Simulator API contract validated before dispatch.")


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



def choose_spot(car_type, planned_minutes, avoid_busy_zone_gates=False, preferred_zone=None):
    """Choose a safe compatible bay across every API-discovered airport zone.

    Level-3 additions:
      * zones whose complete gate group is unavailable are skipped;
      * sensor-quarantined/maintenance bays are never allocated;
      * existing Zone1 -> Zone2 -> ... spillover behavior is preserved.
    """
    zone_names = sorted(
        {str(s.get("zoneParent") or "") for s in spots.values() if str(s.get("zoneParent") or "")},
        key=_natural_zone_key,
    )
    preferred_zone = str(preferred_zone or "")
    if preferred_zone in zone_names:
        zone_names = [preferred_zone] + [z for z in zone_names if z != preferred_zone]
    zone_names.append("")

    busy_zones_with_capacity = []
    blocked_gate_zones = []

    for zone_name in zone_names:
        # If this zone exposes barriers, at least one must be healthy. This turns
        # a broken gate into zone spillover instead of a permanent JIT wait.
        if zone_name and zone_gates.get(zone_name) and not available_route_gates(zone_gates.get(zone_name)):
            blocked_gate_zones.append(zone_name)
            continue

        candidates = []
        for name, spot in spots.items():
            if str(spot.get("zoneParent") or "") != zone_name:
                continue
            if (spot.get("occupied") or spot.get("broken") or spot.get("isUnderMaintenance")
                    or spot.get("sensor_abnormality") or name in sensor_quarantined_spots):
                continue
            if name in reserved_spots:
                continue
            score, reason = smart_spot_score(name, spot, car_type, planned_minutes)
            if score < 0:
                continue
            candidates.append((score, name, reason))

        if not candidates:
            continue

        if avoid_busy_zone_gates and zone_name and zone_crossing_busy(zone_name):
            busy_zones_with_capacity.append(zone_name)
            continue

        candidates.sort(key=lambda x: (-x[0], natural_spot_key(x[1]), x[1]))
        best = candidates[0]
        priority = zone_name or "UNZONED"
        annotations = []
        if busy_zones_with_capacity:
            annotations.append(f"SPILLED_PAST_BUSY={','.join(busy_zones_with_capacity)}")
        if blocked_gate_zones:
            annotations.append(f"SPILLED_PAST_FAILED_GATES={','.join(blocked_gate_zones)}")
        prefix = (" | ".join(annotations) + " | ") if annotations else ""
        entry_pref = f"ENTRY_ZONE_PREFERENCE={preferred_zone} | " if preferred_zone else ""
        return best[1], f"{entry_pref}{prefix}ZONE_PRIORITY={priority} | {best[2]}"

    if avoid_busy_zone_gates and busy_zones_with_capacity:
        return None, f"WAIT_ALL_ZONE_GATES_BUSY={','.join(busy_zones_with_capacity)}"
    if blocked_gate_zones:
        return None, f"NO_ROUTE_HEALTHY_ZONES={','.join(blocked_gate_zones)}"
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
    if s.get("isUnderMaintenance") or s.get("sensor_abnormality") or spot_name in sensor_quarantined_spots:
        return False, "UNDER_MAINTENANCE_OR_SENSOR_QUARANTINE"
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

    # Release only this vehicle's reservation. A sensor/reroute race must never
    # delete a reservation currently owned by another incoming vehicle.
    other_reservation_plate = None
    with state_lock:
        res = reserved_spots.get(spot_name)
        if res and res.get("plate") == plate:
            reserved_spots.pop(spot_name, None)
        elif res:
            other_reservation_plate = res.get("plate")
    if other_reservation_plate:
        log_decision(
            plate, "RESERVATION_HELD_OTHER_PLATE",
            f"{spot_name} still reserved for {other_reservation_plate}",
            "Did not release another vehicle's reservation."
        )
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

    # IMPORTANT LOCK-ORDER RULE:
    # Never call sync_state() while state_lock is held. sync_state() finishes by
    # persisting a full runtime snapshot; persistence serializes on
    # runtime_persist_lock and snapshots under state_lock. Holding state_lock here
    # while waiting for runtime_persist_lock would invert that order and can
    # deadlock against the persistence watchdog.
    with state_lock:
        if entry_active is not None or not entry_queue:
            return
        needs_sync = not spots

    if needs_sync:
        try:
            sync_state()
        except Exception as e:
            log_decision("", "ERROR", f"sync failed during entry: {e}")

        # Check readiness only after sync_state() has fully released its own
        # persistence path. No controller lock is held during simulator I/O.
        with state_lock:
            components_ready = bool(spots)
        if not components_ready:
            print("[WAIT] Level components are not ready yet; entry remains queued.")
            retry = threading.Timer(1.0, process_entry_queue)
            retry.daemon = True
            retry.start()
            return

    # state_lock was deliberately released above, so re-check all admission
    # preconditions before consuming the queued vehicle. Another thread may have
    # legitimately changed entry_active, entry_queue, or the level inventory.
    with state_lock:
        if entry_active is not None or not entry_queue:
            return
        if not spots:
            retry = threading.Timer(1.0, process_entry_queue)
            retry.daemon = True
            retry.start()
            return

        item = entry_queue.pop(0)
        plate = item["plate"]
        car_type = item["car_type"]
        planned = item["planned"]

        # Level-3 entrance failover: do not bind the car to a failed perimeter
        # gate. If at least one sibling is healthy the alias is refreshed and
        # routing continues; if all are unavailable the car safely waits.
        if perimeter_gates and not available_route_gates(perimeter_gates):
            entry_queue.insert(0, item)
            upsert_car(plate, status="WAITING_ENTRY_GATE", decision="NO_HEALTHY_PERIMETER_GATE")
            log_decision(
                plate, "ENTRY_ALL_GATES_UNAVAILABLE",
                "No healthy perimeter entrance gate is currently available.",
                "Vehicle remains at EntrySpot; controller will retry without corrupting the parking lifecycle."
            )
            retry = threading.Timer(0.75, process_entry_queue)
            retry.daemon = True
            retry.start()
            return
        refresh_gate_failover_aliases("entry assignment health check")

        if demo_state.get("live_full_next_arrival"):
            demo_state["live_full_next_arrival"] = False
            spot, reason = None, "ONE-SHOT LIVE FULL-LOT TEST"
        else:
            # Admission-aware selection: if one car already occupies Zone 1's
            # gate path, immediately try Zone 2; then Zone 3; etc.
            spot, reason = choose_spot(
                car_type, planned, avoid_busy_zone_gates=True,
                preferred_zone=item.get("entry_zone"),
            )

        if not spot:
            if str(reason).startswith("NO_ROUTE_HEALTHY_ZONES="):
                entry_queue.insert(0, item)
                upsert_car(plate, status="WAITING_GATE_FAILOVER", decision=reason)
                log_decision(
                    plate, "ZONE_GATE_FAILURE_HOLD", reason,
                    "Safe bays exist only behind unavailable gate groups; waiting for repair/failover instead of declaring the car park full."
                )
                retry = threading.Timer(0.75, process_entry_queue)
                retry.daemon = True
                retry.start()
                return

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

        # Judge/operator-visible Level-3 behaviour: when a lower-numbered zone
        # already owns its gate crossing, the selector deliberately spills this
        # arrival into the next compatible free zone instead of queueing two cars
        # behind the same barrier.  The decision is persisted in SQLite.
        if "SPILLED_PAST_BUSY=" in str(reason):
            log_decision(
                plate, "ZONE_BUSY_SPILLOVER",
                f"Assigned {spot} in {target_zone}; {reason}",
                "Another vehicle owns the skipped zone gate path, so this car uses a different zone."
            )
        if "SPILLED_PAST_FAILED_GATES=" in str(reason):
            log_decision(
                plate, "ZONE_GATE_FAILOVER_ASSIGNMENT",
                f"Assigned {spot} in {target_zone}; {reason}",
                "Broken/maintenance/manual-closed gate path was excluded before assignment."
            )

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
            "entry_spot": item.get("entry_spot"), "entry_zone": item.get("entry_zone"),
            "sent": False, "sent_at": None, "route_retries": 0,
            "required_gates": []
        }
        upsert_car(plate, assigned_spot=spot, status="ASSIGNED", decision=reason)
        log_decision(plate, "ASSIGN", spot, reason)

        try:
            viable, route_problem = route_path_viable(target_zone, perimeter_role="entry")
            if not viable:
                raise RuntimeError(route_problem)
            route_gates = _route_gate_names_for_zone(target_zone, perimeter_role="entry")
            entry_active["required_gates"] = list(route_gates)
            opened = open_route_gates(
                target_zone, reason=f"entry->{spot}", perimeter_role="entry", gate_names=route_gates
            )
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
                failed_required = [g for g in required if not gate_route_available(g)]
                if failed_required:
                    for failed_gate in failed_required:
                        recover_from_gate_failure(failed_gate)
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
        viable, route_problem = route_path_viable(zone_name, perimeter_role=None)
        if not viable:
            with state_lock:
                exit_route_zone.pop(plate, None)
            upsert_car(plate, status="WAITING_EXIT_LANE")
            log_decision(
                plate, "EXIT_SOURCE_GATE_HOLD", route_problem,
                "Vehicle stays in its bay while another gate is repaired or becomes available."
            )
            retry = threading.Timer(0.75, request_exit_lane, args=(plate,))
            retry.daemon = True
            retry.start()
            return
        selected_route = _route_gate_names_for_zone(zone_name, perimeter_role=None)
        route_gates = open_route_gates(
            zone_name, reason=f"to-exit:{plate}", perimeter_role=None, gate_names=selected_route
        )
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



def request_payment_again(plate, factors):
    """Level-3 re-prompt after a suspicious amount, without infinite charging."""
    car = get_car(plate) or {}
    if not car or car.get("payment_status") == "PAID" or not car.get("exit_arrival_time"):
        return False

    with state_lock:
        count = int(suspicious_payment_retries.get(plate, 0) or 0)
        if count >= SUSPICIOUS_PAYMENT_MAX_REPROMPTS:
            log_decision(
                plate, "PAYMENT_REPROMPT_LIMIT",
                f"Suspicious payment retry limit reached ({count}).",
                "Vehicle remains in PAYMENT_HOLD for operator review; no uncontrolled charge loop."
            )
            return False
        suspicious_payment_retries[plate] = count + 1
        stats["payment_reprompts"] += 1
        charge_attempt_counts[plate] = 0
        charge_attempt_scheduled.discard(plate)
        reprompt_number = count + 1

    def _reprompt():
        latest = get_car(plate) or {}
        if latest.get("payment_status") == "PAID":
            return
        if latest.get("status") not in ("AT_EXIT", "PAYMENT_HOLD", "PAYMENT_PENDING"):
            return
        upsert_car(plate, payment_status="WAITING_TO_CHARGE", status="PAYMENT_HOLD")
        schedule_charge_attempt(
            plate,
            float(latest.get("parking_cost") or 0.0),
            float(latest.get("charging_cost") or 0.0),
            trigger=f"Level-3 suspicious-payment re-prompt #{reprompt_number}",
        )

    log_decision(
        plate, "PAYMENT_REPROMPT",
        f"retry={reprompt_number}/{SUSPICIOUS_PAYMENT_MAX_REPROMPTS}; reason={factors}",
        "Incorrect/insufficient amount was not accepted. The correct payment is requested again while the exit stays locked."
    )
    timer = threading.Timer(1.0, _reprompt)
    timer.daemon = True
    timer.start()
    return True


def payment_should_reprompt(factors):
    text = str(factors or "")
    return text.startswith("INSUFFICIENT_FUNDS") or text.startswith("AMOUNT_MISMATCH")


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
    except SimulatorClientError as exc:
        body_lower = exc.body.lower()
        if "should be charged at the exit" in body_lower:
            # This 4xx is itself authoritative rejection of the attempt. It is
            # safe to arm the next bounded backoff immediately; no speculative
            # retry and no 3x generic HTTP backoff are involved.
            upsert_car(plate, payment_status="WAITING_TO_CHARGE", status="AT_EXIT")
            log_decision(
                plate, "CHARGE_REJECTED_NOT_READY",
                f"HTTP {exc.status_code}: {exc.body[:200]}",
                "Explicit simulator rejection; next bounded charge attempt may be armed safely."
            )
            schedule_charge_attempt(
                plate, float(parking_cost), float(charging_cost),
                trigger="explicit charge 4xx rejection",
            )
        else:
            upsert_car(plate, payment_status="CHARGE_ERROR", status="PAYMENT_HOLD")
            log_decision(
                plate, "CHARGE_CLIENT_ERROR",
                f"HTTP {exc.status_code}: {exc.body[:200]}",
                "Permanent simulator 4xx is not retried by the generic API client."
            )
    except Exception as exc:
        upsert_car(plate, payment_status="CHARGE_ERROR", status="PAYMENT_HOLD")
        log_decision(plate, "CHARGE_ERROR", str(exc))



def _exit_release_required_gates(plate):
    """Derive a Level-3 egress path with gate failover.

    The actual ExitSpot webhook identifies the exit zone.  Within that zone and
    at the perimeter, one healthy alternative is selected. A broken sibling is
    excluded from the hard Open interlock, so it cannot deadlock a paid car.
    """
    exit_spot = exit_spot_by_plate.get(plate, "")
    exit_zone = _component_zone(exit_spot) if exit_spot else ""
    required = _route_gate_names_for_zone(exit_zone, perimeter_role="exit")
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
    viable, route_problem = route_path_viable(exit_zone, perimeter_role="exit")
    if not viable:
        upsert_car(plate, status="PAID_WAIT_GATE")
        log_decision(
            plate, "PAID_EXIT_GATE_HOLD", route_problem,
            "Payment remains valid; vehicle is held safely until a healthy exit-gate alternative is available."
        )
        return False

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
                if elapsed > ENTRY_TRANSIT_LOST_SEC:
                    log_decision(
                        plate, "TRANSIT_LOST",
                        f"Car did not arrive at {spot} after {ENTRY_TRANSIT_LOST_SEC}s."
                    )
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

    # Belt-and-braces schema validation for durable/recovered events. The HTTP
    # endpoint also validates new traffic, but old DB rows must be safe to replay.
    plate_required = {"car_spot_action", "payment_made"}
    if event_class in plate_required:
        raw_plate = data.get("CarPlateNumber")
        if not isinstance(raw_plate, str) or not raw_plate.strip():
            log_decision(
                "", "EVENT_MISSING_PLATE",
                f"{event_class}: {data.get('EventId') or '-'}",
                "Dropped to prevent NULL/blank ghost rows in cars table."
            )
            return
        data["CarPlateNumber"] = raw_plate.strip()

    name_required = {
        "gate_action", "component_broken", "component_fixed",
        "fan_action", "exhaust_fan_action", "exhaust_fan_state",
        "light_action", "light_state", "light_state_changed",
    }
    if event_class in name_required:
        raw_name = data.get("Name")
        if not isinstance(raw_name, str) or not raw_name.strip():
            log_decision(
                "", "COMPONENT_EVENT_MISSING_NAME",
                f"{event_class}: {data.get('EventId') or '-'}",
                "Ignored malformed component event instead of creating an empty/None component key."
            )
            return
        data["Name"] = raw_name.strip()

    if event_class == "carbon_monoxide_event":
        raw_zone = data.get("ZoneName") or data.get("Zone")
        if not isinstance(raw_zone, str) or not raw_zone.strip():
            log_decision("", "CO_EVENT_MISSING_ZONE", str(data.get("EventId") or "-"), "Ignored malformed CO event.")
            return

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
                if action == "Closed":
                    active_required = list(entry_active.get("required_gates") or []) if entry_active else []
                    exit_required = list(exit_active.get("required_gates") or []) if exit_active else []
                    if name in active_required or name in exit_required:
                        threading.Thread(target=recover_from_gate_failure, args=(name,), daemon=True).start()

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
                            entry_queue.append({
                                "plate": plate,
                                "car_type": car_type,
                                "planned": planned,
                                "entry_spot": spot_name,
                                "entry_zone": _component_zone(spot_name),
                            })
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
                # Reverse occupancy check: one plate must never be active in two
                # parking bays at once. This catches sensor duplication/double-parking
                # even when each individual bay reports a plausible occupancy count.
                if direction == "CarIn":
                    conn = db()
                    try:
                        other = conn.execute(
                            """SELECT actual_spot FROM cars
                               WHERE plate=? AND actual_spot IS NOT NULL
                                 AND actual_spot<>?
                                 AND status NOT IN ('LEFT','ESCAPED_UNPAID')
                               LIMIT 1""",
                            (plate, spot_name),
                        ).fetchone()
                    finally:
                        conn.close()

                    if other and other["actual_spot"]:
                        other_spot = str(other["actual_spot"])
                        log_anomaly(
                            plate, "DOUBLE_PARKING",
                            f"Plate reported CarIn at {spot_name} while still recorded in {other_spot}"
                        )
                        record_alert(
                            alert_key=f"DOUBLE_PARK:{plate}:{spot_name}",
                            alert_type="DOUBLE PARKING",
                            severity="CRITICAL",
                            plate=plate,
                            reason=f"Vehicle seen entering {spot_name} while still occupying {other_spot}.",
                            event_time=server_time,
                        )
                        quarantine_spot_for_sensor_abnormality(spot_name, "Double-parking conflict")
                        quarantine_spot_for_sensor_abnormality(other_spot, "Double-parking conflict")

                # A second different vehicle reported entering an already occupied
                # bay is an impossible single-bay sensor condition. Quarantine the
                # bay for future allocation, but preserve this event so the current
                # trip can still be reconciled instead of crashing the workflow.
                if direction == "CarIn" and spot_name in spots and spots[spot_name].get("occupied"):
                    existing_here = None
                    conn = db()
                    try:
                        row = conn.execute(
                            "SELECT plate FROM cars WHERE actual_spot=? AND status NOT IN ('LEFT','ESCAPED_UNPAID') LIMIT 1",
                            (spot_name,),
                        ).fetchone()
                        existing_here = row["plate"] if row else None
                    finally:
                        conn.close()
                    if existing_here and existing_here != plate:
                        quarantine_spot_for_sensor_abnormality(
                            spot_name,
                            f"Impossible occupancy conflict: sensor reported {plate} entering while {existing_here} is still recorded in the bay",
                        )

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
                    suspicious_payment_retries.pop(plate, None)
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
                    f"FACTORS: {factors}. Exit release remains LOCKED. "
                    "NO PAYMENT = NO LEAVE. A legitimate corrected/retry payment is requested when appropriate."
                )
                record_alert(
                    alert_key=f"PAYMENT_FAIL:{plate}:{data.get('EventId') or data.get('SequenceId') or datetime.now().timestamp()}",
                    alert_type="PAYMENT FAILURE",
                    severity="HIGH",
                    plate=plate,
                    reason=f"Payment rejected: {factors}",
                    event_time=data.get("ServerDateTime") or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                )
                if payment_should_reprompt(factors):
                    request_payment_again(plate, factors)

        elif event_class in (
            "sensor_abnormality",
            "spot_sensor_abnormality",
            "parking_spot_sensor_abnormality",
            "parking_sensor_abnormality",
        ):
            sensor_spot = str(data.get("SpotName") or data.get("Name") or data.get("ParkingSpotName") or "")
            detail = str(data.get("Problem") or data.get("Reason") or data.get("Message") or "Sensor abnormality reported by simulator")
            if sensor_spot in spots:
                quarantine_spot_for_sensor_abnormality(sensor_spot, detail)
            else:
                log_decision("", "SENSOR_EVENT_UNKNOWN_SPOT", sensor_spot or "<missing>", detail)

        elif event_class == "component_broken":
            name = str(data.get("Name") or "")
            problem = str(data.get("Problem") or "Require Maintenance")
            with state_lock:
                for source in (spots, gates, fans, lights, entry_spots, exit_spots, leave_spots, parking_nodes):
                    if name in source:
                        source[name]["broken"] = True
                if name:
                    alarms[name] = {"name": name, "problem": problem}
            log_decision("", "COMPONENT_BROKEN", name,
                         "Failure came from simulator webhook; component isolated from automatic use.")

            if name in gates:
                threading.Thread(target=recover_from_gate_failure, args=(name,), daemon=True).start()
            if name in spots and "sensor" in problem.lower():
                quarantine_spot_for_sensor_abnormality(name, problem)

        elif event_class == "component_fixed":
            name = str(data.get("Name") or "")
            with state_lock:
                for source in (spots, gates, fans, lights, entry_spots, exit_spots, leave_spots, parking_nodes):
                    if name in source:
                        source[name]["broken"] = False
                        source[name]["isUnderMaintenance"] = False
                        if isinstance(source[name], dict):
                            source[name].pop("sensor_abnormality", None)
                alarms.pop(name, None)
                maintenance_requested.discard(name)
                sensor_quarantined_spots.pop(name, None)
            if name in gates:
                refresh_gate_failover_aliases(f"gate fixed: {name}")
                threading.Thread(target=ensure_keep_open_gates, daemon=True).start()
                # A paid car may have been waiting because every sibling gate was down.
                with state_lock:
                    waiting_exit_plate = exit_active.get("plate") if exit_active else None
                if waiting_exit_plate:
                    threading.Thread(target=ensure_gate_open_and_leave, args=(waiting_exit_plate,), daemon=True).start()
                threading.Thread(target=process_entry_queue, daemon=True).start()
            log_decision("", "COMPONENT_FIXED", name,
                         "Simulator webhook cleared the cached failure/maintenance state and re-enabled route selection.")

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

            log_penalty(reason, fine, data)
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
    ensure_event_workers_started()
    raw = request.get_data(cache=True) or b""
    fp = request_fingerprint(raw)
    remote = request.headers.get("X-Forwarded-For", request.remote_addr or "")

    if len(raw) > MAX_WEBHOOK_BYTES:
        log_network_security("INVALID", detail=f"payload too large: {len(raw)} bytes", fingerprint=fp,
                             remote_addr=remote, raw_preview=raw[:SECURITY_PAYLOAD_PREVIEW].decode("utf-8", "replace"))
        return jsonify({"status": "invalid_request", "reason": "payload_too_large"}), 413

    if not request.is_json:
        log_network_security("INVALID", detail="Content-Type is not JSON", fingerprint=fp,
                             remote_addr=remote, raw_preview=raw[:SECURITY_PAYLOAD_PREVIEW].decode("utf-8", "replace"))
        return jsonify({"status": "invalid_request", "reason": "json_required"}), 400

    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        log_network_security("INVALID", detail="JSON body must be an object", fingerprint=fp,
                             remote_addr=remote, raw_preview=raw[:SECURITY_PAYLOAD_PREVIEW].decode("utf-8", "replace"))
        return jsonify({"status": "invalid_request", "reason": "object_required"}), 400

    event_class = data.get("EventClass")
    if not isinstance(event_class, str) or not event_class.strip() or len(event_class) > 120:
        log_network_security("INVALID", data, "missing/invalid EventClass", fp, remote)
        return jsonify({"status": "invalid_request", "reason": "event_class"}), 400

    seq = data.get("SequenceId")
    if seq is not None and (isinstance(seq, bool) or not isinstance(seq, int) or seq < 0):
        log_network_security("INVALID", data, "SequenceId must be a non-negative integer", fp, remote)
        return jsonify({"status": "invalid_request", "reason": "sequence_id"}), 400

    # Event-specific minimum schema. Invalid network calls are rejected before
    # entering the durable operational event queue and remain visible in Security.
    if event_class in ("car_spot_action", "payment_made"):
        raw_plate = data.get("CarPlateNumber")
        if not isinstance(raw_plate, str) or not raw_plate.strip():
            log_network_security("INVALID", data, f"{event_class} missing CarPlateNumber", fp, remote)
            return jsonify({"status": "invalid_request", "reason": "car_plate"}), 400

    if event_class in (
        "gate_action", "component_broken", "component_fixed",
        "fan_action", "exhaust_fan_action", "exhaust_fan_state",
        "light_action", "light_state", "light_state_changed",
    ):
        raw_name = data.get("Name")
        if not isinstance(raw_name, str) or not raw_name.strip():
            log_network_security("INVALID", data, f"{event_class} missing Name", fp, remote)
            return jsonify({"status": "invalid_request", "reason": "component_name"}), 400

    if event_class == "carbon_monoxide_event":
        raw_zone = data.get("ZoneName") or data.get("Zone")
        if not isinstance(raw_zone, str) or not raw_zone.strip():
            log_network_security("INVALID", data, "carbon_monoxide_event missing zone", fp, remote)
            return jsonify({"status": "invalid_request", "reason": "zone_name"}), 400

    if VERBOSE_WEBHOOK_LOG:
        print("\n[WEBHOOK]", data)
    else:
        print(f"[WEBHOOK] class={event_class} seq={seq} id={data.get('EventId') or '-'}")

    # Signature integrity handling. Unsigned events remain accepted for simulator
    # compatibility; a supplied signature that does not match is a tamper event.
    sig_result = verify_webhook_signature(data)
    if sig_result is False:
        with state_lock:
            stats["webhooks_rejected_sig"] += 1
        log_network_security("TAMPERED", data, "Signature mismatch", fp, remote)
        log_decision("", "WEBHOOK_REJECTED", "Signature mismatch",
                     "Integrity check failed; event was not persisted for operational processing.")
        return jsonify({"status": "signature_invalid"}), 403

    if sig_result is True:
        with state_lock:
            stats["webhooks_verified"] += 1
        signature_label = "verified"
    else:
        with state_lock:
            stats["webhooks_unsigned_accepted"] += 1
        signature_label = "unsigned_compatible"

    inserted, event_id, duplicate_reason = save_event(data, verified=(sig_result is True), fingerprint=fp)
    if not inserted:
        if duplicate_reason == "event_id_payload_mismatch":
            category = "TAMPERED"
        elif duplicate_reason.startswith("sequence_replay"):
            category = "REPLAY"
        else:
            category = "DUPLICATE"
        log_network_security(category, data, duplicate_reason, fp, remote)
        log_decision("", "WEBHOOK_DUPLICATE", f"{event_id}: {duplicate_reason}",
                     "Idempotency guard prevented the same network event being acted on twice.")
        return jsonify({"status": "duplicate_ignored", "reason": duplicate_reason}), 200

    queued = enqueue_persisted_event(event_id, seq)
    with state_lock:
        stats["event_queue_peak"] = max(stats.get("event_queue_peak", 0), event_queue.qsize())
        stats["events_accepted"] = stats.get("events_accepted", 0) + 1
        if not queued:
            stats["event_queue_spill_to_db"] = stats.get("event_queue_spill_to_db", 0) + 1

    # Preserve the simulator-facing 200 response used by the working Level-2/3 flow.
    # Processing is asynchronous, but acceptance is already durable before this reply.
    return jsonify({
        "status": "queued" if queued else "durable_db_pending",
        "signature": signature_label,
        "event_id": event_id,
        "queue_depth": event_queue.qsize(),
    }), 200


# =====================================================================
# WEB DASHBOARD + ROLES
# =====================================================================
MAINTENANCE_USER = os.getenv("PARKMIND_MAINT_USER", "maintenance")
MAINTENANCE_PASSWORD = os.getenv("PARKMIND_MAINT_PASSWORD", "maintenance")

USERS = {
    "admin": {"password": "admin", "role": "Admin"},
    "operator": {"password": "operator", "role": "Operator"},
    MAINTENANCE_USER: {"password": MAINTENANCE_PASSWORD, "role": "Maintenance"},
}

LOGIN_HTML = """
<!doctype html>
<title>PARKMIND v3 — Login</title>
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
<h1>PARKMIND v3.7</h1>
<p class="sub">Pretty Little Hackers — Level 3 Airport Control Center</p>
<div style="margin:10px 0">
<span class="badge">Signature Verified</span>
<span class="badge">Sequence Gap Detection</span>
<span class="badge">Multi-Factor Payment</span>
<span class="badge">Smart Spot AI</span>
<span class="badge">Maintenance Control</span>
</div>
{% if error %}<p class="err">{{error}}</p>{% endif %}
<form method="post">
<input name="username" placeholder="Username" required>
<input name="password" type="password" placeholder="Password" required>
<button>Login</button>
</form>
<p class="sub">admin/admin · operator/operator · maintenance/maintenance</p>
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
<a href="/control">Control Centre</a>
{% if is_admin %}<a href="/maintenance/">Maintenance</a><a href="/admin/security">Security</a><a href="/admin/database">Database</a><a href="/export/cars">Export</a>{% endif %}
<a href="/logout">Logout</a>
</div>
</header>

<div class="page">

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

<div class="card" style="margin-top:10px">
<div class="alert-title"><h2 style="margin:0">Component Availability Summary</h2><span class="mini">Live API/webhook cache</span></div>
<table><tr><th>Component</th><th>Total</th><th>Available</th><th>Broken</th><th>Maintenance</th><th>Unavailable %</th></tr>
{% for c in component_summary %}<tr><td><b>{{c.label}}</b></td><td>{{c.total}}</td><td style="color:#86efac"><b>{{c.available}}</b></td><td style="color:#fca5a5">{{c.broken}}</td><td style="color:#fbbf24">{{c.maintenance}}</td><td>{{c.unavailable_pct}}%</td></tr>{% endfor %}
</table>
<div class="mini" style="margin-top:7px">Events accepted: {{stats.events_accepted}} · queue: {{event_queue_depth}} waiting · peak {{stats.event_queue_peak}} · durable DB spillovers {{stats.event_queue_spill_to_db}}</div>
</div>

<div class="card" style="margin-top:10px">
<div class="alert-title"><h2 style="margin:0">Arrivals · last 10 minutes</h2><span class="mini">traffic pulse</span></div>
<div style="display:flex;align-items:flex-end;gap:5px;height:92px;margin-top:8px">
{% for b in arrival_bars %}
<div style="flex:1;min-width:10px;text-align:center" title="{{b.label}} · {{b.count}} arrival(s)">
  <div style="height:{{b.height}}px;background:#38bdf8;border-radius:4px 4px 2px 2px"></div>
  <div class="mini" style="font-size:8px;margin-top:3px">{{b.short}}</div>
</div>
{% endfor %}
</div>
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

  // Keep the dashboard live without deleting text an operator is currently typing.
  function scheduleDashboardRefresh() {
    setTimeout(() => {
      const active = document.activeElement;
      const editing = !!(active && active.matches && active.matches("input, textarea, select"));
      if (editing) {
        scheduleDashboardRefresh();
        return;
      }
      panels.forEach((panel) => {
        saved[panel.dataset.panel] = panel.open;
      });
      sessionStorage.setItem(storageKey, JSON.stringify(saved));
      sessionStorage.setItem("parkmind-scroll-y", String(window.scrollY || 0));
      window.location.reload();
    }, {{dashboard_refresh_ms}});
  }
  scheduleDashboardRefresh();

  const y = parseInt(sessionStorage.getItem("parkmind-scroll-y") || "0", 10);
  if (y > 0) window.scrollTo(0, y);
})();
</script>
</body>
</html>
"""



CONTROL_HTML = """
<!doctype html>
<html>
<head>
<title>PARKMIND v3.7 · Airport Control Centre</title>
<style>
*{box-sizing:border-box}body{font-family:'Segoe UI',Arial;margin:0;background:#07111f;color:#e5e7eb}
header{position:sticky;top:0;z-index:10;background:#0f172a;border-bottom:1px solid #263244;padding:13px 18px;display:flex;justify-content:space-between;align-items:center;gap:12px}
a{color:#67e8f9;text-decoration:none}.brand{font-size:20px;font-weight:900;color:#67e8f9}.sub{font-size:11px;color:#94a3b8}.page{max-width:1650px;margin:auto;padding:14px}
.metrics{display:grid;grid-template-columns:repeat(6,minmax(110px,1fr));gap:8px}.metric,.card{background:#0f172a;border:1px solid #263244;border-radius:12px}.metric{padding:10px}.metric b{font-size:21px}.metric span{display:block;color:#94a3b8;font-size:10px;text-transform:uppercase;margin-top:3px}
.toolbar{display:flex;gap:7px;flex-wrap:wrap;align-items:center}.toolbar input,.toolbar select{background:#07111f;color:#e5e7eb;border:1px solid #334155;border-radius:8px;padding:7px 9px}.card{padding:12px;margin-top:10px}.card h2{font-size:14px;margin:0;color:#cbd5e1;text-transform:uppercase;letter-spacing:.6px}
.section-head{display:flex;justify-content:space-between;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:9px}.group{border:1px solid #263244;border-radius:11px;margin:9px 0;overflow:hidden}.group-head{background:#111c2e;padding:9px 11px;display:flex;justify-content:space-between;align-items:center;gap:8px}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:8px;padding:8px}.component{background:#081421;border:1px solid #203047;border-radius:10px;padding:10px}.row{display:flex;justify-content:space-between;gap:8px;align-items:flex-start}.name{font-weight:850;font-size:14px}.mini{font-size:10px;color:#94a3b8;margin-top:3px}.bad{color:#fca5a5}.warn{color:#fbbf24}.good{color:#86efac}.cyan{color:#67e8f9}.badge{font-size:9px;padding:3px 6px;border-radius:999px;background:#1e293b;display:inline-block;margin:1px}.badge.manual{background:#4c1d95;color:#ddd6fe}.badge.open{background:#064e3b;color:#a7f3d0}.badge.closed{background:#3f1d24;color:#fecaca}.badge.auto{background:#164e63;color:#a5f3fc}
button{border:0;border-radius:7px;padding:6px 8px;color:#fff;font-size:10px;font-weight:700;cursor:pointer;margin:2px 1px;background:#0369a1}.openbtn{background:#047857}.closebtn{background:#b91c1c}.autobtn{background:#475569}.bulk{background:#7c3aed}.disabled{opacity:.45}.controls{margin-top:8px;display:flex;flex-wrap:wrap}.note{padding:9px;border-radius:9px;background:#082f49;border:1px solid #0e7490;color:#bae6fd;font-size:11px;margin:10px 0}.warning{background:#3b2109;border-color:#92400e;color:#fde68a}
table{width:100%;border-collapse:collapse;font-size:11px}th,td{padding:7px;border-bottom:1px solid #263244;text-align:left}th{font-size:9px;text-transform:uppercase;color:#94a3b8}.scroll{max-height:300px;overflow:auto}.hiddenByFilter{display:none!important}
@media(max-width:900px){.metrics{grid-template-columns:repeat(2,1fr)}header{align-items:flex-start}.grid{grid-template-columns:1fr}}
</style>
</head>
<body>
<header><div><div class="brand">PARKMIND · AIRPORT CONTROL CENTRE</div><div class="sub">Level 3 manual override · live API state · SQLite audit</div></div><div class="toolbar"><span class="badge">{{role}} · {{user}}</span><a href="/">Dashboard</a><a href="/maintenance/">Maintenance</a>{% if is_admin %}<a href="/admin/security">Security</a><a href="/admin/database">Database</a>{% endif %}<a href="/logout">Logout</a></div></header>
<div class="page">
<div class="metrics">
<div class="metric"><b class="cyan">{{gate_total}}</b><span>Total gates</span></div>
<div class="metric"><b class="good">{{gate_open}}</b><span>Open / opening</span></div>
<div class="metric"><b>{{gate_closed}}</b><span>Closed / closing</span></div>
<div class="metric"><b class="warn">{{gate_manual}}</b><span>Manual holds</span></div>
<div class="metric"><b class="bad">{{gate_unavailable}}</b><span>Broken / maintenance</span></div>
<div class="metric"><b>{{audit_rows|length}}</b><span>Recent override audit</span></div>
</div>
<div class="note"><b>Level 3 behaviour:</b> OPEN/CLOSE creates a persistent manual hold. AUTO returns the component to PARKMIND policy. If the selected gate is the active entry alias, PARKMIND first transfers the entry role to another healthy perimeter gate before closing it. The final usable perimeter entrance is never allowed to close.</div>
<div class="toolbar" style="margin-top:10px"><input id="searchBox" placeholder="Search component / zone..." oninput="applyFilter()"><select id="typeFilter" onchange="applyFilter()"><option value="all">All components</option><option value="gate">Gates</option><option value="fan">Fans</option><option value="light">Lights</option></select><select id="zoneFilter" onchange="applyFilter()"><option value="all">All zones</option>{% for z in all_zones %}<option value="{{z}}">{{z}}</option>{% endfor %}</select></div>

<div class="card control-section" data-type="gate"><div class="section-head"><h2>🚧 Gate Overrides</h2><div><form method="post" action="/control/bulk/gate/open"><input type="hidden" name="scope" value="ALL"><button class="bulk">OPEN ALL HEALTHY</button></form><form method="post" action="/control/bulk/gate/close"><input type="hidden" name="scope" value="ALL"><button class="closebtn">CLOSE ALL SAFE</button></form><form method="post" action="/control/bulk/gate/auto"><input type="hidden" name="scope" value="ALL"><button class="autobtn">AUTO ALL</button></form></div></div>
{% for group in gate_groups %}<div class="group component-group" data-type="gate" data-zone="{{group.zone}}"><div class="group-head"><div><b>{{group.zone}}</b><div class="mini">{{group.rows|length}} gate(s) · {{group.open_count}} open · {{group.manual_count}} manual</div></div><div><form method="post" action="/control/bulk/gate/open"><input type="hidden" name="scope" value="{{group.zone}}"><button class="openbtn">Open group</button></form><form method="post" action="/control/bulk/gate/close"><input type="hidden" name="scope" value="{{group.zone}}"><button class="closebtn">Close group safe</button></form><form method="post" action="/control/bulk/gate/auto"><input type="hidden" name="scope" value="{{group.zone}}"><button class="autobtn">Auto group</button></form></div></div><div class="grid">
{% for g in group.rows %}<div class="component filter-item" data-type="gate" data-zone="{{g.zone}}" data-search="{{g.name}} {{g.zone}} {{g.state}} {{g.override}}"><div class="row"><div><div class="name">{{g.name}}</div><div class="mini">{{g.zone}} · health {{g.health}}% · uses {{g.uses}}</div></div><div style="text-align:right"><span class="badge {{'open' if g.state in ['Open','Opening'] else 'closed'}}">{{g.state}}</span><span class="badge {{'manual' if g.override!='AUTO' else 'auto'}}">{{g.override}}</span>{% if g.entry_alias %}<span class="badge">ENTRY</span>{% endif %}{% if g.exit_alias %}<span class="badge">EXIT</span>{% endif %}{% if g.broken %}<span class="badge bad">BROKEN</span>{% endif %}{% if g.maintenance %}<span class="badge warn">MAINT</span>{% endif %}</div></div><div class="controls"><form method="post" action="/control/gate/{{g.name}}/open"><button class="openbtn">↑ OPEN + HOLD</button></form><form method="post" action="/control/gate/{{g.name}}/close"><button class="closebtn">↓ CLOSE + HOLD</button></form><form method="post" action="/control/gate/{{g.name}}/auto"><button class="autobtn">AUTO</button></form></div></div>{% endfor %}
</div></div>{% endfor %}</div>

<div class="card control-section" data-type="fan"><div class="section-head"><h2>🌀 Exhaust Fan Overrides</h2><div><form method="post" action="/control/bulk/fan/on"><input type="hidden" name="scope" value="ALL"><button class="openbtn">ALL ON</button></form><form method="post" action="/control/bulk/fan/off"><input type="hidden" name="scope" value="ALL"><button class="closebtn">ALL OFF SAFE</button></form><form method="post" action="/control/bulk/fan/auto"><input type="hidden" name="scope" value="ALL"><button class="autobtn">AUTO ALL</button></form></div></div><div class="grid">{% for f in fan_rows %}<div class="component filter-item" data-type="fan" data-zone="{{f.zone}}" data-search="{{f.name}} {{f.zone}} {{f.override}}"><div class="row"><div><div class="name">{{f.name}}</div><div class="mini">{{f.zone}} · CO risk {{f.risk}} · health {{f.health}}%</div></div><div><span class="badge {{'open' if f.on else 'closed'}}">{{'ON' if f.on else 'OFF'}}</span><span class="badge {{'manual' if f.override!='AUTO' else 'auto'}}">{{f.override}}</span></div></div><div class="controls"><form method="post" action="/control/fan/{{f.name}}/on"><button class="openbtn">ON + HOLD</button></form><form method="post" action="/control/fan/{{f.name}}/off"><button class="closebtn">OFF + HOLD</button></form><form method="post" action="/control/fan/{{f.name}}/auto"><button class="autobtn">AUTO</button></form></div></div>{% endfor %}</div></div>

<div class="card control-section" data-type="light"><div class="section-head"><h2>💡 Light Overrides</h2><div><form method="post" action="/control/bulk/light/on"><input type="hidden" name="scope" value="ALL"><button class="openbtn">ALL ON</button></form><form method="post" action="/control/bulk/light/off"><input type="hidden" name="scope" value="ALL"><button class="closebtn">ALL OFF</button></form><form method="post" action="/control/bulk/light/auto"><input type="hidden" name="scope" value="ALL"><button class="autobtn">AUTO ALL</button></form></div></div><div class="grid">{% for l in light_rows %}<div class="component filter-item" data-type="light" data-zone="{{l.zone}}" data-search="{{l.name}} {{l.zone}} {{l.group}} {{l.override}}"><div class="row"><div><div class="name">{{l.name}}</div><div class="mini">{{l.zone}} · group {{l.group}} · health {{l.health}}%</div></div><div><span class="badge {{'open' if l.on else 'closed'}}">{{'ON' if l.on else 'OFF'}}</span><span class="badge {{'manual' if l.override!='AUTO' else 'auto'}}">{{l.override}}</span></div></div><div class="controls"><form method="post" action="/control/light/{{l.name}}/on"><button class="openbtn">ON + HOLD</button></form><form method="post" action="/control/light/{{l.name}}/off"><button class="closebtn">OFF + HOLD</button></form><form method="post" action="/control/light/{{l.name}}/auto"><button class="autobtn">AUTO</button></form></div></div>{% endfor %}</div></div>

<div class="card"><div class="section-head"><h2>🗃 Manual Override Audit · SQLite</h2><span class="mini">Every Control Centre command is stored permanently</span></div><div class="scroll"><table><tr><th>Time</th><th>User</th><th>Component</th><th>Zone</th><th>Action</th><th>Result</th><th>Before</th><th>After</th><th>Detail</th></tr>{% for r in audit_rows %}<tr><td>{{r.created_at}}</td><td>{{r.username}} · {{r.role}}</td><td><b>{{r.component_type}}:{{r.component_name}}</b></td><td>{{r.zone_parent or '-'}}</td><td>{{r.action}}</td><td class="{{'good' if r.result=='SUCCESS' else 'bad'}}">{{r.result}}</td><td>{{r.previous_state}}</td><td>{{r.state_after}}</td><td>{{r.detail}}</td></tr>{% else %}<tr><td colspan="9">No manual commands recorded yet.</td></tr>{% endfor %}</table></div></div>
</div>
<script>
function applyFilter(){const q=(document.getElementById('searchBox').value||'').toLowerCase();const t=document.getElementById('typeFilter').value;const z=document.getElementById('zoneFilter').value;document.querySelectorAll('.filter-item').forEach(el=>{const okQ=!q||(el.dataset.search||'').toLowerCase().includes(q);const okT=t==='all'||el.dataset.type===t;const okZ=z==='all'||el.dataset.zone===z;el.classList.toggle('hiddenByFilter',!(okQ&&okT&&okZ));});document.querySelectorAll('.control-section').forEach(el=>el.style.display=(t==='all'||el.dataset.type===t)?'block':'none');}
</script>
</body></html>
"""


SECURITY_HTML = """
<!doctype html><html><head><title>PARKMIND — Network Security</title>
<style>
body{font-family:'Segoe UI',Arial;margin:0;background:#0f172a;color:#e5e7eb}.wrap{padding:22px}.top{display:flex;justify-content:space-between;align-items:center;gap:10px;flex-wrap:wrap}
a{color:#22d3ee}.cards{display:grid;grid-template-columns:repeat(5,1fr);gap:10px;margin:14px 0}.card{background:#111827;border:1px solid #334155;border-radius:12px;padding:14px}.v{font-size:25px;font-weight:800}.k{font-size:10px;color:#94a3b8;text-transform:uppercase}.bad{color:#fca5a5}.warn{color:#fbbf24}.ok{color:#86efac}table{width:100%;border-collapse:collapse;font-size:11px}th,td{padding:7px;border-bottom:1px solid #263244;text-align:left;vertical-align:top}th{color:#94a3b8;text-transform:uppercase;font-size:9px}.mono{font-family:Consolas,monospace;word-break:break-all}.pill{font-size:9px;font-weight:800;padding:3px 7px;border-radius:999px;background:#1f2937}.TAMPERED{color:#fca5a5}.INVALID{color:#fbbf24}.DUPLICATE,.REPLAY{color:#93c5fd}@media(max-width:900px){.cards{grid-template-columns:repeat(2,1fr)}}
</style></head><body><div class="wrap">
<div class="top"><div><h1 style="margin:0">Parking Network Security</h1><div style="color:#94a3b8">Invalid, duplicated, replayed and tampered requests</div></div><div><a href="/">← Dashboard</a></div></div>
<div class="cards">
<div class="card"><div class="v">{{counts.total}}</div><div class="k">Logged Requests</div></div>
<div class="card"><div class="v bad">{{counts.tampered}}</div><div class="k">Tampered</div></div>
<div class="card"><div class="v warn">{{counts.invalid}}</div><div class="k">Invalid</div></div>
<div class="card"><div class="v">{{counts.duplicate}}</div><div class="k">Duplicates</div></div>
<div class="card"><div class="v">{{counts.replay}}</div><div class="k">Sequence Replays</div></div>
</div>
<div class="card"><h3 style="margin-top:0">Recent security events</h3><table><tr><th>Time</th><th>Category</th><th>Event</th><th>Seq</th><th>Class</th><th>Source</th><th>Detail</th><th>Fingerprint</th><th>Preview</th></tr>
{% for r in rows %}<tr><td>{{r.received_at}}</td><td><span class="pill {{r.category}}">{{r.category}}</span></td><td>{{r.event_id or '-'}}</td><td>{{r.sequence_id if r.sequence_id is not none else '-'}}</td><td>{{r.event_class or '-'}}</td><td>{{r.remote_addr or '-'}}</td><td>{{r.detail}}</td><td class="mono">{{(r.request_fingerprint or '')[:16]}}…</td><td class="mono">{{r.payload_preview}}</td></tr>{% else %}<tr><td colspan="9">No rejected/duplicate parking-network requests logged.</td></tr>{% endfor %}</table></div>
</div></body></html>
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


def ensure_web_runtime_ready():
    """Make web/login use safe whether launched by `python file.py` or a WSGI/Flask runner.

    Previously the database was guaranteed to be initialized only from __main__
    or after the first webhook. That meant a valid login could redirect to the
    dashboard before SQLite tables existed when the app was started by another
    runner. The existing event-worker bootstrap is idempotent, so reuse it here.
    """
    try:
        ensure_event_workers_started()
        return True
    except Exception as exc:
        # Do not turn a recoverable simulator/runtime-state startup issue into a
        # broken login page. At minimum create/migrate the local dashboard DB.
        print(f"[WEB STARTUP] Full runtime bootstrap failed: {exc}")
        try:
            init_db()
            return True
        except Exception as db_exc:
            print(f"[WEB STARTUP] Database initialization failed: {db_exc}")
            return False


@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        # Friendly input handling: accidental capitals/spaces should not make the
        # demo look broken. Password remains exact/case-sensitive.
        username = str(request.form.get("username", "") or "").strip().lower()
        password = str(request.form.get("password", "") or "")
        u = USERS.get(username)
        if u and hmac.compare_digest(str(u["password"]), password):
            if not ensure_web_runtime_ready():
                error = "PARKMIND database could not be initialized. Check the terminal log."
                return render_template_string(LOGIN_HTML, error=error)

            session.clear()
            session["user"] = username
            session["role"] = u["role"]

            # Maintenance credentials go straight to the maintenance console.
            if u["role"] == "Maintenance":
                return redirect(url_for("maintenance.dashboard"))

            return redirect(url_for("dashboard"))
        error = "Invalid username or password"
    return render_template_string(LOGIN_HTML, error=error)


@app.route("/operator/login", methods=["GET", "POST"])
def operator_login():
    error = None
    if request.method == "POST":
        username = str(request.form.get("username", "operator") or "").strip().lower()
        password = str(request.form.get("password", "") or "")
        u = USERS.get("operator")
        if username == "operator" and u and hmac.compare_digest(str(u["password"]), password):
            if not ensure_web_runtime_ready():
                error = "PARKMIND database could not be initialized. Check the terminal log."
                return render_template_string(OPERATOR_LOGIN_HTML, error=error)

            session.clear()
            session["user"] = "operator"
            session["role"] = "Operator"
            return redirect(url_for("dashboard"))
        error = "Invalid operator password"
    return render_template_string(OPERATOR_LOGIN_HTML, error=error)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/")
def dashboard():
    if not require_login():
        return redirect(url_for("login"))

    # Also covers a browser that still has a valid session cookie after the
    # controller process was restarted by a WSGI/Flask runner.
    if not ensure_web_runtime_ready():
        return ("PARKMIND database startup failed. Check the terminal log.", 503)

    if session.get("role") == "Maintenance":
        return redirect(url_for("maintenance.dashboard"))

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
            "broken": bool(l.get("broken", False) or n in alarms),
            "maintenance": bool(l.get("isUnderMaintenance", False)),
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

        def _component_counts(label, rows, available_fn):
            total = len(rows)
            broken = sum(1 for r in rows if r.get("broken"))
            maintenance = sum(1 for r in rows if r.get("maintenance"))
            available = sum(1 for r in rows if available_fn(r))
            unavailable = max(0, total - available)
            return {
                "label": label, "total": total, "available": available,
                "broken": broken, "maintenance": maintenance,
                "unavailable_pct": round((unavailable / total) * 100, 1) if total else 0,
            }

        component_summary = [
            _component_counts("Parking spots", spot_rows, lambda r: not r["busy"] and not r["broken"] and not r["maintenance"]),
            _component_counts("Gates", gate_rows, lambda r: not r["broken"] and not r["maintenance"]),
            _component_counts("Exhaust fans", fan_rows, lambda r: not r["broken"] and not r["maintenance"]),
            _component_counts("Lights", light_rows, lambda r: not r.get("broken", False) and not r.get("maintenance", False)),
        ]

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
        component_summary=component_summary,
        event_queue_depth=event_queue.qsize(),
        dashboard_refresh_ms=DASHBOARD_REFRESH_MS,
        is_admin=require_admin(),
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


@app.route("/admin/database")
def admin_database_status():
    if not session.get("user"):
        return redirect(url_for("login"))
    if session.get("role") != "Admin":
        return "Forbidden", 403
    conn = db()
    tables = [
        "events", "cars", "decisions", "spot_stats", "anomalies", "fraud_log",
        "penalties", "alerts", "network_security_log", "runtime_state",
        "runtime_state_history", "component_state", "component_state_history",
        "controller_queue_state", "persistence_log", "manual_override_log"
    ]
    counts = {}
    for table in tables:
        try:
            counts[table] = conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()["n"]
        except Exception:
            counts[table] = "-"
    latest = conn.execute(
        "SELECT saved_at,reason,component_count,queue_item_count,state_hash "
        "FROM persistence_log ORDER BY id DESC LIMIT 1"
    ).fetchone()
    health = conn.execute(
        """SELECT component_type, COUNT(*) total,
                  SUM(CASE WHEN available=1 THEN 1 ELSE 0 END) available,
                  SUM(broken) broken, SUM(under_maintenance) maintenance
           FROM component_state GROUP BY component_type ORDER BY component_type"""
    ).fetchall()
    conn.close()
    html = """
    <!doctype html><title>PARKMIND Database</title>
    <style>body{font-family:Segoe UI,Arial;background:#0f172a;color:#e5e7eb;padding:24px}a{color:#38bdf8}table{border-collapse:collapse;width:100%;margin:16px 0;background:#111827}th,td{border:1px solid #334155;padding:9px;text-align:left}th{background:#1e293b}.card{background:#1e293b;padding:16px;border-radius:14px;margin:12px 0}</style>
    <p><a href="/">← Dashboard</a> &nbsp; <a href="/control">Control Centre</a> &nbsp; <a href="/admin/security">Security</a></p>
    <h1>Database Persistence Status</h1>
    <div class="card"><b>Latest checkpoint:</b>
    {% if latest %}{{latest['saved_at']}} — {{latest['reason']}} — {{latest['component_count']}} component rows — {{latest['queue_item_count']}} queued items<br><small>{{latest['state_hash']}}</small>{% else %}No checkpoint yet{% endif %}
    </div>
    <h2>Persisted tables</h2><table><tr><th>Table</th><th>Rows</th></tr>{% for name,n in counts.items() %}<tr><td>{{name}}</td><td>{{n}}</td></tr>{% endfor %}</table>
    <h2>Current component state from SQLite</h2><table><tr><th>Type</th><th>Total</th><th>Available</th><th>Broken</th><th>Maintenance</th></tr>{% for r in health %}<tr><td>{{r['component_type']}}</td><td>{{r['total']}}</td><td>{{r['available'] or 0}}</td><td>{{r['broken'] or 0}}</td><td>{{r['maintenance'] or 0}}</td></tr>{% endfor %}</table>
    """
    return render_template_string(html, counts=counts, latest=latest, health=health)


@app.route("/admin/security")
def admin_security():
    if not require_admin():
        return redirect(url_for("login"))
    conn = db()
    rows = [dict(r) for r in conn.execute(
        "SELECT * FROM network_security_log ORDER BY id DESC LIMIT 250"
    ).fetchall()]
    agg = {r["category"]: r["n"] for r in conn.execute(
        "SELECT category, COUNT(*) AS n FROM network_security_log GROUP BY category"
    ).fetchall()}
    conn.close()
    counts = {
        "total": sum(agg.values()),
        "tampered": agg.get("TAMPERED", 0),
        "invalid": agg.get("INVALID", 0),
        "duplicate": agg.get("DUPLICATE", 0),
        "replay": agg.get("REPLAY", 0),
    }
    return render_template_string(SECURITY_HTML, rows=rows, counts=counts)


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


def _control_role_allowed():
    return bool(session.get("user")) and session.get("role") in {"Admin", "Operator", "Maintenance"}


def _override_label(kind, name):
    with state_lock:
        value = manual_overrides.get(kind, {}).get(name)
    if value is True:
        return "OPEN" if kind == "gate" else "ON"
    if value is False:
        return "CLOSED" if kind == "gate" else "OFF"
    return "AUTO"


def _restore_override(kind, name, previous):
    with state_lock:
        bucket = manual_overrides.setdefault(kind, {})
        if previous is None:
            bucket.pop(name, None)
        else:
            bucket[name] = previous
        if kind == "gate":
            if previous is False:
                manual_gate_closed.add(name)
            else:
                manual_gate_closed.discard(name)


def control_gate_action(name, action):
    """Level-3 manual gate command with perimeter-entry failover before CLOSE."""
    global ENTRY_GATE
    name = str(name or "").strip()
    action = str(action or "").strip().lower()
    if name not in gates:
        return False, "Unknown gate in live discovery cache."
    if action not in {"open", "close", "auto"}:
        return False, "Invalid gate action."

    previous_override = gate_manual_override(name)
    previous_state = f"state={gates[name].get('state','?')};override={_override_label('gate', name)}"

    if action == "auto":
        with state_lock:
            manual_overrides["gate"].pop(name, None)
            manual_gate_closed.discard(name)
        refresh_gate_failover_aliases(f"manual AUTO for {name}")
        ensure_keep_open_gates()
        return True, f"{name} returned to AUTO control."

    if not gate_safe(name):
        return False, f"{name} is broken or under maintenance; movement command blocked."

    if action == "open":
        with state_lock:
            manual_overrides["gate"][name] = True
            manual_gate_closed.discard(name)
        try:
            if str(gates[name].get("state") or "") not in ("Open", "Opening"):
                if not open_gate(name, manual=True):
                    raise RuntimeError("open command was blocked")
            refresh_gate_failover_aliases(f"manual OPEN for {name}")
            return True, f"{name} is MANUAL OPEN; automation cannot close it until AUTO/CLOSE."
        except Exception as exc:
            _restore_override("gate", name, previous_override)
            refresh_gate_failover_aliases(f"rollback manual OPEN for {name}")
            return False, f"Could not open {name}: {exc}"

    # CLOSE: if this is the current entry alias, transfer that role first.
    if name == ENTRY_GATE:
        alternatives = [g for g in available_route_gates(perimeter_gates) if g != name]
        if not alternatives:
            return False, f"{name} is the last usable perimeter entrance; CLOSE blocked to preserve car spawning."

    with state_lock:
        manual_overrides["gate"][name] = False
        manual_gate_closed.add(name)

    if name == ENTRY_GATE:
        refresh_gate_failover_aliases(f"manual close requested for active entry alias {name}")
        if name == ENTRY_GATE:
            _restore_override("gate", name, previous_override)
            return False, f"No alternative entry alias could be established; {name} remains available."

    try:
        if str(gates[name].get("state") or "") not in ("Closed", "Closing"):
            if not close_gate(name, force=True, manual=True):
                raise RuntimeError("close command was blocked")
        refresh_gate_failover_aliases(f"manual CLOSED for {name}")
        return True, f"{name} is MANUAL CLOSED; automation will route around it until AUTO/OPEN."
    except Exception as exc:
        _restore_override("gate", name, previous_override)
        refresh_gate_failover_aliases(f"rollback manual CLOSE for {name}")
        return False, f"Could not close {name}: {exc}"


def control_fan_action(name, action):
    name = str(name or "").strip()
    action = str(action or "").strip().lower()
    if name not in fans:
        return False, "Unknown fan in live discovery cache."
    if action not in {"on", "off", "auto"}:
        return False, "Invalid fan action."
    previous = manual_overrides["fan"].get(name)
    if action == "auto":
        with state_lock:
            manual_overrides["fan"].pop(name, None)
        apply_co_policy(str(fans[name].get("zoneParent") or ""))
        return True, f"{name} returned to automatic CO control."
    if fans[name].get("broken") or fans[name].get("isUnderMaintenance"):
        return False, f"{name} is broken or under maintenance."
    zone = str(fans[name].get("zoneParent") or "")
    risk = str(zones.get(zone, {}).get("risk") or "").strip().lower()
    if action == "off" and risk and risk not in {"safe", "low", "normal", "ok"}:
        return False, f"Manual OFF blocked: {zone} CO risk is {zones.get(zone, {}).get('risk')}; ventilation must remain available."
    desired = action == "on"
    with state_lock:
        manual_overrides["fan"][name] = desired
    try:
        if bool(fans[name].get("isOn")) != desired:
            if not set_fan(name, desired, "Level-3 Control Centre manual override"):
                raise RuntimeError("fan command was blocked")
        return True, f"{name} is MANUAL {'ON' if desired else 'OFF'}."
    except Exception as exc:
        _restore_override("fan", name, previous)
        return False, f"Fan command failed: {exc}"


def control_light_action(name, action):
    name = str(name or "").strip()
    action = str(action or "").strip().lower()
    if name not in lights:
        return False, "Unknown light in live discovery cache."
    if action not in {"on", "off", "auto"}:
        return False, "Invalid light action."
    previous = manual_overrides["light"].get(name)
    if action == "auto":
        with state_lock:
            manual_overrides["light"].pop(name, None)
        apply_light_policy()
        return True, f"{name} returned to automatic day/night control."
    if lights[name].get("broken") or lights[name].get("isUnderMaintenance"):
        return False, f"{name} is broken or under maintenance."
    desired = action == "on"
    with state_lock:
        manual_overrides["light"][name] = desired
    try:
        if bool(lights[name].get("isOn")) != desired:
            if not set_light(name, desired, "Level-3 Control Centre manual override"):
                raise RuntimeError("light command was blocked")
        return True, f"{name} is MANUAL {'ON' if desired else 'OFF'}."
    except Exception as exc:
        _restore_override("light", name, previous)
        return False, f"Light command failed: {exc}"


def _control_state_text(kind, name):
    source = {"gate": gates, "fan": fans, "light": lights}.get(kind, {})
    item = source.get(name, {})
    if kind == "gate":
        return f"state={item.get('state','?')};override={_override_label(kind,name)}"
    return f"state={'ON' if item.get('isOn') else 'OFF'};override={_override_label(kind,name)}"


def _control_apply_and_audit(kind, name, action, persist=True):
    before = _control_state_text(kind, name)
    handler = {"gate": control_gate_action, "fan": control_fan_action, "light": control_light_action}.get(kind)
    if not handler:
        return False, "Unsupported component type."
    try:
        success, detail = handler(name, action)
    except Exception as exc:
        success, detail = False, f"Unhandled manual-control error: {exc}"
    after = _control_state_text(kind, name)
    try:
        log_manual_override(kind, name, action, before, "SUCCESS" if success else "FAILED", detail, after)
    except Exception as exc:
        log_decision("", "MANUAL_AUDIT_FAIL", f"{kind}:{name}:{exc}", detail)
    if success and persist:
        checkpoint_manual_override(f"{kind}:{name}:{action}")
    log_decision("", f"CONTROL_{kind.upper()}_{action.upper()}", f"{name}: {detail}", f"user={session.get('user')} role={session.get('role')}")
    return success, detail


@app.route("/control")
def control_center():
    if not _control_role_allowed():
        return redirect(url_for("login"))
    with state_lock:
        gate_rows = []
        for name, g in sorted(gates.items(), key=lambda kv: (_natural_zone_key(str(kv[1].get("zoneParent") or "PERIMETER")), _natural_zone_key(kv[0]))):
            zone = str(g.get("zoneParent") or "") or "PERIMETER"
            gate_rows.append({
                "name": name, "zone": zone, "state": str(g.get("state") or "Unknown"),
                "broken": bool(g.get("broken") or name in alarms),
                "maintenance": bool(g.get("isUnderMaintenance")),
                "health": component_health_score(name, "gate"),
                "uses": int(g.get("usage_count") or 0),
                "override": _override_label("gate", name),
                "entry_alias": name == ENTRY_GATE, "exit_alias": name == EXIT_GATE,
            })
        fan_rows = []
        for name, f in sorted(fans.items(), key=lambda kv: (_natural_zone_key(str(kv[1].get("zoneParent") or "")), _natural_zone_key(kv[0]))):
            zone = str(f.get("zoneParent") or "") or "UNASSIGNED"
            fan_rows.append({"name": name, "zone": zone, "on": bool(f.get("isOn")),
                             "broken": bool(f.get("broken") or name in alarms), "maintenance": bool(f.get("isUnderMaintenance")),
                             "health": component_health_score(name, "fan"), "override": _override_label("fan", name),
                             "risk": str(zones.get(zone, {}).get("risk") or "Unknown")})
        light_rows = []
        for name, l in sorted(lights.items(), key=lambda kv: (_natural_zone_key(str(kv[1].get("zoneParent") or "")), _natural_zone_key(kv[0]))):
            zone = str(l.get("zoneParent") or "") or "UNASSIGNED"
            light_rows.append({"name": name, "zone": zone, "group": str(l.get("group") or "-"), "on": bool(l.get("isOn")),
                               "broken": bool(l.get("broken") or name in alarms), "maintenance": bool(l.get("isUnderMaintenance")),
                               "health": component_health_score(name, "light"), "override": _override_label("light", name)})

    gate_groups = []
    for zone in sorted({r["zone"] for r in gate_rows}, key=_natural_zone_key):
        rows = [r for r in gate_rows if r["zone"] == zone]
        gate_groups.append({"zone": zone, "rows": rows,
                            "open_count": sum(1 for r in rows if r["state"] in ("Open", "Opening")),
                            "manual_count": sum(1 for r in rows if r["override"] != "AUTO")})

    conn = db()
    try:
        audit_rows = [dict(r) for r in conn.execute(
            "SELECT * FROM manual_override_log ORDER BY id DESC LIMIT 100"
        ).fetchall()]
    finally:
        conn.close()

    all_zones = sorted({r["zone"] for r in gate_rows + fan_rows + light_rows}, key=_natural_zone_key)
    return render_template_string(
        CONTROL_HTML, user=session.get("user"), role=session.get("role"), is_admin=session.get("role") == "Admin",
        gate_rows=gate_rows, fan_rows=fan_rows, light_rows=light_rows, gate_groups=gate_groups,
        all_zones=all_zones, audit_rows=audit_rows, gate_total=len(gate_rows),
        gate_open=sum(1 for r in gate_rows if r["state"] in ("Open", "Opening")),
        gate_closed=sum(1 for r in gate_rows if r["state"] in ("Closed", "Closing")),
        gate_manual=sum(1 for r in gate_rows if r["override"] != "AUTO"),
        gate_unavailable=sum(1 for r in gate_rows if r["broken"] or r["maintenance"]),
    )


@app.route("/control/<kind>/<name>/<action>", methods=["POST"])
def control_component(kind, name, action):
    if not _control_role_allowed():
        return redirect(url_for("login"))
    kind = str(kind or "").lower()
    _control_apply_and_audit(kind, name, action)
    return redirect(url_for("control_center"))


@app.route("/control/bulk/<kind>/<action>", methods=["POST"])
def control_bulk(kind, action):
    if not _control_role_allowed():
        return redirect(url_for("login"))
    kind = str(kind or "").lower()
    action = str(action or "").lower()
    scope = str(request.form.get("scope") or "ALL").strip()
    source = {"gate": gates, "fan": fans, "light": lights}.get(kind)
    allowed = {"gate": {"open", "close", "auto"}, "fan": {"on", "off", "auto"}, "light": {"on", "off", "auto"}}
    if source is None or action not in allowed.get(kind, set()):
        return ("Invalid bulk manual-control request", 400)

    with state_lock:
        names = [n for n, item in source.items() if scope == "ALL" or (str(item.get("zoneParent") or "") or ("PERIMETER" if kind == "gate" else "UNASSIGNED")) == scope]
    successes = 0
    failures = 0
    for name in names:
        ok, _ = _control_apply_and_audit(kind, name, action, persist=False)
        successes += 1 if ok else 0
        failures += 0 if ok else 1
    log_decision("", "CONTROL_BULK", f"{kind}:{action} scope={scope} success={successes} failed={failures}", f"user={session.get('user')}")
    checkpoint_manual_override(f"bulk:{kind}:{action}:{scope}")
    return redirect(url_for("control_center"))


@app.route("/sync", methods=["POST"])
def sync_route():
    if not require_admin():
        return ("Admin only", 403)
    safe_sync_state("manual")
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
    except Exception as e:
        log_decision("", "MANUAL_GATE_ERROR", f"{name}: {e}")
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
    except Exception as e:
        log_decision("", "MANUAL_FAN_ERROR", f"{name}: {e}")
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
    except Exception as e:
        log_decision("", "MANUAL_LIGHT_ERROR", f"{name}: {e}")
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
    except Exception as e:
        log_decision("", "MANUAL_LIGHT_GROUP_ERROR", f"{group}: {e}")
    return redirect(url_for("dashboard"))


@app.route("/component/<kind>/<name>/repair", methods=["POST"])
def manual_component_repair(kind, name):
    if not require_login():
        return redirect(url_for("login"))
    source = {"gate": gates, "fan": fans, "spot": spots}.get(kind)
    if source is None or name not in source:
        return ("Unknown/unsupported component from simulator discovery cache", 404)
    try:
        repair_component(name)
    except Exception as e:
        log_decision("", "MANUAL_REPAIR_ERROR", f"{kind}:{name}: {e}")
    return redirect(url_for("dashboard"))


@app.route("/car/<plate>/exit", methods=["POST"])
def manual_exit(plate):
    if not require_login():
        return redirect(url_for("login"))
    threading.Thread(target=request_exit_lane, args=(plate,), daemon=True).start()
    return redirect(url_for("dashboard"))


@app.route("/car/<plate>/reassign", methods=["POST"])
def reassign_route_hold(plate):
    """Operator recovery for a vehicle isolated in ROUTE_HOLD."""
    if not require_login():
        return redirect(url_for("login"))

    car = get_car(plate)
    if not car:
        return ("Unknown car", 404)

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


# [INNOVATION #14] — CSV EXPORT
@app.route("/export/cars")
def export_cars():
    if not require_admin():
        return ("Admin only", 403)
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

            with state_lock:
                manual_overrides["fan"][name] = (action == "on")
            checkpoint_manual_override(f"maintenance-fan:{name}:{action}")

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

            with state_lock:
                manual_overrides["light"][name] = (action == "on")
            checkpoint_manual_override(f"maintenance-light:{name}:{action}")

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

            checkpoint_manual_override(f"maintenance-gate:{name}:{action}")

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


def startup_simulator_connection():
    """Connect/sync the simulator without blocking the web login server."""
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


# =====================================================================
# MAIN
# =====================================================================
if __name__ == "__main__":
    ensure_event_workers_started()

    # Start background watchdogs (innovations #4 + #11)
    threading.Thread(target=reservation_ttl_watchdog, daemon=True).start()
    threading.Thread(target=stuck_state_watchdog, daemon=True).start()
    threading.Thread(target=entry_transit_watchdog, daemon=True).start()
    threading.Thread(target=close_idle_gates, daemon=True).start()
    threading.Thread(target=exit_stuck_alert_watchdog, daemon=True).start()
    threading.Thread(target=maintenance_watchdog, daemon=True).start()
    threading.Thread(target=light_policy_watchdog, daemon=True).start()

    # IMPORTANT: do not make the login page wait for simulator retries.
    # The controller can come online first; simulator sync continues in parallel.
    threading.Thread(
        target=startup_simulator_connection,
        daemon=True,
        name="parkmind-simulator-startup",
    ).start()

    print("\n============================================")
    print("  PARKMIND v3.7 — LEVEL 3 Airport API-Safe Entry Routing + Full-Database Edition")
    print("  Team: Pretty Little Hackers")
    print("  Dashboard: http://127.0.0.1:8000")
    print("  Webhook:   http://127.0.0.1:8000/webhook")
    print("  Login:     admin/admin · operator/operator · maintenance/maintenance")
    print("  Maintenance: http://127.0.0.1:8000/maintenance/")
    print("  Control:     http://127.0.0.1:8000/control")
    print("  API-derived entry alias:", ENTRY_GATE, "(derived from /list-barriers)")
    print("  API-derived exit alias: ", EXIT_GATE, "(derived from /list-barriers)")
    print("  Entry control: ZONE-SPILLOVER (busy first zone -> next zone; no internal pile-up)")
    print("  Exit control: TWO-STAGE + HARD PAYMENT LOCK (no payment = no leavepark)")
    print("  Entry spawn guard: ALL healthy perimeter gates kept open for multi-entrance spawning")
    print("  Level 3: sensor quarantine + suspicious-payment re-prompt + gate failover")
    print("  Traffic: durable ordered event queue + SQLite WAL + pooled simulator HTTP")
    print("  Security: /admin/security logs invalid, duplicate, replayed and tampered requests")
    print("  Database: /admin/database shows persisted tables + current component health")
    print("  Persistence: ALL runtime/component/queue state checkpointed to SQLite")
    print("  Gate retries: debounced (will not spam OPEN while already Opening/Open)")
    print("============================================\n")

    app.run(host="0.0.0.0", port=WEB_PORT, threaded=True)
