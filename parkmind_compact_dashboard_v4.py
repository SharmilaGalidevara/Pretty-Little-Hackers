# =====================================================================
#  PARKMIND v2.0  —  "The Wow Factor" Edition
#  Team: Pretty Little Hackers  |  Level: 1
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

from flask import Flask, request, jsonify, render_template_string, redirect, url_for, session, Response
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
from datetime import datetime, timedelta
from urllib.parse import quote
from collections import defaultdict, Counter

# ---------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------
SIM_BASE = "http://127.0.0.1:9898/api/v1"
SIM_USER = "admin"
SIM_PASSWORD = "admin"

# Preferred names can be overridden without editing code:
#   $env:PARKMIND_ENTRY_GATE="gate0"
#   $env:PARKMIND_EXIT_GATE="gate1"
ENTRY_GATE = os.getenv("PARKMIND_ENTRY_GATE", "gateA")
EXIT_GATE  = os.getenv("PARKMIND_EXIT_GATE", "gateB")

# Level 1 commonly uses gateA (entry) and gateB (exit), but we verify these
# names against /list-barriers at runtime. If either configured gate is absent,
# PARKMIND falls back to safe auto-resolution instead of wasting seconds on 404s.
GATE_AUTO_DISCOVERY = True

WEB_PORT = 8000

# Smart-selection tuning
RESERVATION_TTL_SEC = 25           # auto-release a reserved spot after 25s
STUCK_STATE_TIMEOUT_SEC = 60      # watchdog retries ops stuck > 60s
ANOMALY_PARKING_MULTIPLIER = 10   # 10x planned duration = anomaly
HEALTH_SCORE_THRESHOLD_USES = 50  # rough estimate for component "wear"
PAYMENT_TIMEOUT_SEC = 20            # unresolved exit payment -> alert
EXIT_HOLD_RECOVERY_SEC = 180        # try to clear exit lane after prolonged unresolved payment
EXIT_STUCK_ALERT_SEC = 15           # create/update CRITICAL alert after 15s at EXIT without departure
ENTRY_GATE_IDLE_CLOSE_SEC = 1.0     # keep entry gate open briefly for back-to-back arrivals
GATE_RELEASE_FALLBACK_SEC = 0.8     # route car if gate webhook is late
ENTRY_TRANSIT_TIMEOUT_SEC = 8        # car got a route command but never reached its spot
ENTRY_TRANSIT_MAX_RETRIES = 2        # resend assigned destination before aborting that one car

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
app.secret_key = "pretty-little-hackers-level1-v2"

token = None
token_lock = threading.Lock()

spots = {}             # name -> dict (live state)
gates = {}             # name -> dict (live state)
zones = {}             # name -> dict (CO2 / risk preview)
lights = {}            # name -> dict (future-ready)

reserved_spots = {}    # spot_name -> {"plate", "reserved_at"}
entry_queue = []
exit_queue  = []          # paid cars waiting for final gate release
to_exit_queue = []        # parked cars waiting for the single exit approach

# IMPORTANT PIPELINE MODEL:
# entry_active = ONLY the vehicle currently crossing the entry gate.
# in_transit   = vehicles that already cleared ENTRY1 and are driving to reserved bays.
entry_active = None
in_transit = {}           # plate -> {"spot", "sent_at", "route_retries"}

exit_active  = None
exit_lane_plate = None    # only one car may approach/occupy EXIT at a time

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
}

# Track last seen SequenceId for gap detection
last_sequence_id = None
sequence_lock = threading.Lock()


# =====================================================================
# DATABASE
# =====================================================================
def db():
    conn = sqlite3.connect("parkmind_v2.db", timeout=10)
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

    alert_cols = {row[1] for row in conn.execute("PRAGMA table_info(alerts)").fetchall()}
    if "stay_seconds" not in alert_cols:
        conn.execute("ALTER TABLE alerts ADD COLUMN stay_seconds INTEGER DEFAULT 0")
    if "exit_arrival_time" not in alert_cols:
        conn.execute("ALTER TABLE alerts ADD COLUMN exit_arrival_time TEXT")
    if "stuck_seconds" not in alert_cols:
        conn.execute("ALTER TABLE alerts ADD COLUMN stuck_seconds INTEGER DEFAULT 0")

    conn.commit()
    conn.close()


def seconds_between(start_text, end_text):
    """Exact non-negative seconds between two simulator timestamps."""
    if not start_text or not end_text:
        return 0
    try:
        start_dt = datetime.strptime(str(start_text)[:19], "%Y-%m-%d %H:%M:%S")
        end_dt = datetime.strptime(str(end_text)[:19], "%Y-%m-%d %H:%M:%S")
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
        current_exit_lane = exit_lane_plate
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

    Level 1 commonly sends Signature=None, so those events must remain usable.
    We accept them as UNSIGNED and record that fact rather than falsely claiming
    they were verified.
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
    """
    Verify configured gate names against /list-barriers.

    Rules:
      1) If configured ENTRY_GATE and EXIT_GATE both exist, KEEP them.
      2) If one/both are missing, auto-resolve from real barrier names.
      3) Prefer zone-attached barriers for the main car-park entry/exit pair.
      4) Never use the same barrier for both directions.
    """
    global ENTRY_GATE, EXIT_GATE

    names = [str(g.get("name")) for g in barrier_data if g.get("name")]
    if not names:
        raise RuntimeError("Simulator returned no barriers from /list-barriers")

    # Perfect case: configured names really exist.
    if ENTRY_GATE in names and EXIT_GATE in names and ENTRY_GATE != EXIT_GATE:
        print(f"[GATE VERIFY] entry={ENTRY_GATE}, exit={EXIT_GATE}, available={names}")
        return

    # Build a deterministic fallback list.
    zone_attached = sorted(
        [
            str(g.get("name"))
            for g in barrier_data
            if g.get("name") and str(g.get("zoneParent") or "").strip()
        ]
    )
    candidates = zone_attached if len(zone_attached) >= 2 else sorted(names)

    old_entry, old_exit = ENTRY_GATE, EXIT_GATE

    if ENTRY_GATE not in names:
        ENTRY_GATE = candidates[0]

    if EXIT_GATE not in names or EXIT_GATE == ENTRY_GATE:
        remaining = [n for n in candidates if n != ENTRY_GATE]
        if not remaining:
            remaining = [n for n in sorted(names) if n != ENTRY_GATE]
        if not remaining:
            raise RuntimeError(f"Could not resolve distinct entry/exit gates from {names}")
        EXIT_GATE = remaining[0]

    msg = (
        f"Configured gates missing/invalid. Auto-resolved "
        f"entry={ENTRY_GATE}, exit={EXIT_GATE}; available={names}; "
        f"previous=({old_entry},{old_exit})"
    )
    print("[GATE DISCOVERY]", msg)
    try:
        log_decision(
            "", "GATE_DISCOVERY", msg,
            "Resolved from the simulator's actual /list-barriers response."
        )
    except Exception:
        pass


def sync_state():
    with state_lock:
        park_data = sim_request("GET", "/list-parking-spots").json()
        barrier_data = sim_request("GET", "/list-barriers").json()
        if GATE_AUTO_DISCOVERY:
            resolve_gate_names(barrier_data)

        # Preserve local analytics + reservation info across syncs.
        prev_reservations = dict(reserved_spots)
        prev_spots = {name: dict(value) for name, value in spots.items()}

        spots.clear()
        for s in park_data:
            if s.get("purpose") == "Park":
                spots[s["name"]] = {
                    **s,
                    "occupied": detected_count(s.get("detectedCars")) > 0,
                    "usage_count": prev_spots.get(s["name"], {}).get("usage_count", 0),
                }

        # Restore reservations for spots that still exist and aren't actually occupied.
        reserved_spots.clear()
        for spot_name, res in prev_reservations.items():
            if spot_name in spots and not spots[spot_name]["occupied"]:
                reserved_spots[spot_name] = res

        gates.clear()
        for g in barrier_data:
            gates[g["name"]] = dict(g)

        # Future-ready: try zones/lights; tolerate absence.
        try:
            zone_data = sim_request("GET", "/list-zones").json()
            zones.clear()
            for z in zone_data:
                zones[z["name"]] = dict(z)
        except Exception:
            pass

        try:
            lights_data = sim_request("GET", "/list-lights").json()
            lights.clear()
            for l in lights_data:
                lights[l["name"]] = dict(l)
        except Exception:
            pass

    print(f"[SYNC] {len(spots)} spots, {len(gates)} gates, {len(zones)} zones, {len(lights)} lights.")
    log_decision("", "SYNC", f"Loaded {len(spots)} spots and {len(gates)} gates")


# =====================================================================
# COMPONENT HEALTH (INNOVATION #15)
# =====================================================================
def component_health_score(name, kind):
    """0–100 health score based on observed usage. (Predictive maintenance preview.)"""
    if kind == "spot":
        s = spots.get(name, {})
        if s.get("broken"):
            return 0
        if s.get("isUnderMaintenance"):
            return 25
        uses = s.get("usage_count", 0)
        return max(0, 100 - (uses * 100 // HEALTH_SCORE_THRESHOLD_USES))
    if kind == "gate":
        g = gates.get(name, {})
        if g.get("broken"):
            return 0
        if g.get("isUnderMaintenance"):
            return 25
        uses = g.get("usage_count", 0)
        return max(0, 100 - (uses * 100 // HEALTH_SCORE_THRESHOLD_USES))
    return 100


# =====================================================================
# GATE HELPERS
# =====================================================================
def gate_safe(name):
    g = gates.get(name)
    if not g:
        return True
    return not g.get("broken", False) and not g.get("isUnderMaintenance", False)


def open_gate(name):
    if not gate_safe(name):
        log_decision("", "BLOCKED", f"Refused to open {name}: broken/maintenance",
                     "Gate safety check failed.")
        return False
    sim_request("POST", f"/barrier-gates/{quote(name, safe='')}/open")
    log_decision("", "GATE_OPEN_CMD", name,
                 "Automatic gate-open command sent. Waiting for physical gate event.")
    return True


def close_gate(name):
    if not gate_safe(name):
        log_decision("", "BLOCKED", f"Refused to close {name}: broken/maintenance",
                     "Gate safety check failed.")
        return False
    sim_request("POST", f"/barrier-gates/{quote(name, safe='')}/close")
    log_decision("", "GATE_CLOSE_CMD", name,
                 "Automatic gate-close command sent to simulator.")
    return True


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
    """Gate A/B are not allowed to stay open with no active vehicle movement."""
    while True:
        time.sleep(0.5)

        try:
            with state_lock:
                entry_idle = entry_active is None and not entry_queue
                exit_idle = exit_active is None
                entry_state = str(gates.get(ENTRY_GATE, {}).get("state") or "")
                exit_state = str(gates.get(EXIT_GATE, {}).get("state") or "")

            if entry_idle and entry_state == "Open":
                close_gate(ENTRY_GATE)

            if exit_idle and exit_state == "Open":
                close_gate(EXIT_GATE)

        except Exception as e:
            print("[IDLE GATE WATCHDOG]", e)


def gate_confirmed_open(name):
    """True only when a real gate webhook has reported Opening/Open."""
    state = str(gates.get(name, {}).get("state") or "")
    return state in ("Opening", "Open")


def close_entry_if_idle():
    """Avoid wasteful close→open cycles when another car is already queued."""
    with state_lock:
        if entry_active is not None or entry_queue:
            return
    try:
        close_gate(ENTRY_GATE)
    except Exception as e:
        log_decision("", "ENTRY_IDLE_CLOSE_ERROR", str(e))


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
    """
    Paid-vehicle exit fallback. Retries the leavepark routing command up to
    4 times if the normal gate-action path and the API client's retries fail.
    """
    global exit_active

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


def choose_spot(car_type, planned_minutes):
    candidates = []
    for name, s in spots.items():
        if s.get("occupied") or s.get("broken") or s.get("isUnderMaintenance"):
            continue
        if name in reserved_spots:
            continue
        score, reason = smart_spot_score(name, s, car_type, planned_minutes)
        if score < 0:
            continue
        candidates.append((score, name, reason))

    if not candidates:
        return None, ""
    candidates.sort(key=lambda x: -x[0])
    best = candidates[0]
    return best[1], best[2]


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

    # Entry throughput is controlled by ENTRY1 CarOut, not by PARKED.
    # This allows multiple vehicles to travel toward different reserved bays concurrently.


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

            # Do not confuse "level still loading" with "parking full".
            # Keep the car queued and retry instead of incorrectly sending it away.
            if not spots:
                print("[WAIT] Level components are not ready yet; entry remains queued.")
                threading.Timer(1.0, process_entry_queue).start()
                return

        item = entry_queue.pop(0)
        plate = item["plate"]
        car_type = item["car_type"]
        planned = item["planned"]

        # Live full-lot test is deliberately ONE-SHOT so it cannot
        # accidentally make every subsequent vehicle leave.
        if demo_state.get("live_full_next_arrival"):
            demo_state["live_full_next_arrival"] = False
            spot, reason = None, "ONE-SHOT LIVE FULL-LOT TEST"
            log_decision(
                plate, "LIVE_DEMO_FULL",
                "Admin armed a one-car full-lot test.",
                "Only this arrival is treated as full; normal admission resumes immediately."
            )
        else:
            spot, reason = choose_spot(car_type, planned)

        if not spot:
            upsert_car(plate, status="LEFT_FULL")
            log_decision(plate, "NO_SPACE",
                         "No safe compatible parking spot. Sending car away.",
                         "All candidates occupied/broken/reserved.")
            try:
                send_car(plate, "leavepark")
            except Exception as e:
                log_decision(plate, "ERROR", f"leavepark failed: {e}")
            threading.Timer(0.2, process_entry_queue).start()
            return

        reserved_spots[spot] = {"plate": plate, "reserved_at": time.time()}
        entry_active = {
            "plate": plate,
            "spot": spot,
            "sent": False,
            "sent_at": None,
            "route_retries": 0
        }
        upsert_car(plate, assigned_spot=spot, status="ASSIGNED", decision=reason)
        log_decision(plate, "ASSIGN", f"{spot}", reason)

        try:
            # FAST PATH: if the barrier is physically Opening/Open already,
            # keep it open and route the next queued vehicle immediately.
            if gate_confirmed_open(ENTRY_GATE):
                send_car(plate, spot)
                mark_entry_dispatched(plate, spot)
                log_decision(
                    plate, "ENTRY_FAST_PATH",
                    f"{ENTRY_GATE} already open; routed directly to {spot}.",
                    "Avoided an unnecessary close/open barrier cycle."
                )
            else:
                opened = open_gate(ENTRY_GATE)
                if opened is False:
                    raise RuntimeError(f"Entry gate {ENTRY_GATE} failed safety/open check")

                watchdog = threading.Timer(
                    1.0, entry_gate_open_watchdog, args=(plate, spot, 2)
                )
                watchdog.daemon = True
                watchdog.start()

                release_fallback = threading.Timer(
                    GATE_RELEASE_FALLBACK_SEC,
                    delayed_entry_release,
                    args=(plate, spot)
                )
                release_fallback.daemon = True
                release_fallback.start()

            # Last-resort queue watchdog.
            threading.Timer(30.0, process_entry_queue).start()

        except Exception as e:
            # Critical deadlock fix: never leave entry_active stuck forever.
            if entry_active and entry_active.get("plate") == plate:
                entry_active = None
            res = reserved_spots.get(spot)
            if res and res.get("plate") == plate:
                reserved_spots.pop(spot, None)

            upsert_car(plate, status="ENTRY_RETRY")
            log_decision(
                plate, "ENTRY_RECOVERY",
                f"Entry handling failed: {e}",
                "Cleared active assignment and queued a safe retry."
            )
            entry_queue.insert(0, item)
            threading.Timer(5.0, process_entry_queue).start()


def schedule_exit(plate, planned_minutes):
    """Schedule a request to use the exit approach. Only one car is allowed
    to approach/occupy the exit at a time, so one payment problem cannot
    create a pile-up at EXIT."""
    seconds = max(1, int(planned_minutes)) * 60
    log_decision(plate, "TIMER", f"Exit requested in {seconds}s")
    timer = threading.Timer(seconds, request_exit_lane, args=(plate,))
    timer.daemon = True
    timer.start()


def request_exit_lane(plate):
    global exit_lane_plate
    car = get_car(plate)
    if not car or car.get("status") not in ("PARKED", "WAITING_EXIT_LANE"):
        return

    with state_lock:
        if exit_lane_plate is None:
            exit_lane_plate = plate
            dispatch_now = True
        else:
            dispatch_now = False
            if plate not in to_exit_queue:
                to_exit_queue.append(plate)
                upsert_car(plate, status="WAITING_EXIT_LANE")
                log_decision(
                    plate, "EXIT_QUEUE",
                    f"Waiting safely in parking spot; exit is occupied by {exit_lane_plate}",
                    "Exit-lane serialization prevents a queue collision."
                )

    if dispatch_now:
        send_to_exit(plate)


def dispatch_next_exit_lane():
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
        threading.Thread(target=send_to_exit, args=(next_plate,), daemon=True).start()


def send_to_exit(plate):
    car = get_car(plate)
    if not car or car["status"] not in ("PARKED", "WAITING_EXIT_LANE"):
        return
    try:
        send_car(plate, "exit")
        upsert_car(plate, status="TO_EXIT")
        append_journey(plate, "sent_to_exit")
    except Exception as e:
        # Release lane ownership if the command itself failed.
        global exit_lane_plate
        with state_lock:
            if exit_lane_plate == plate:
                exit_lane_plate = None
        log_decision(plate, "ERROR", f"Could not send to exit: {e}")
        dispatch_next_exit_lane()


# =====================================================================
# [INNOVATION #3 + #9] — MULTI-FACTOR PAYMENT VERIFICATION
# =====================================================================
def calculate_charge(plate, exit_time_str):
    car = get_car(plate)
    if not car:
        return (1, 1.0, 0.0)

    parked_str = car.get("parked_time")
    minutes = 1
    try:
        parked = datetime.strptime(parked_str, "%Y-%m-%d %H:%M:%S")
        exit_time = datetime.strptime(exit_time_str, "%Y-%m-%d %H:%M:%S")
        minutes = max(1, math.ceil((exit_time - parked).total_seconds() / 60))
    except Exception:
        minutes = max(1, int(car.get("planned_minutes") or 1))

    # Anomaly check: parking for 10x planned duration = flag.
    planned = int(car.get("planned_minutes") or 0)
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
    if car.get("status") not in ("AT_EXIT", "PAYMENT_PENDING", "WAITING_TO_CHARGE", "PAYMENT_HOLD"):
        log_fraud(plate,
                  f"BAD_STATUS — status={car.get('status')}",
                  received_amount)
        return False, f"BAD_STATUS({car.get('status')})"
    factors.append("STATUS_OK=✓")

    return True, " | ".join(factors)


def payment_timeout_watch(plate):
    """If payment never resolves, keep the gate closed and keep later cars
    parked in their bays instead of sending everyone into the exit lane."""
    time.sleep(PAYMENT_TIMEOUT_SEC)
    car = get_car(plate)
    if not car:
        return
    if car.get("payment_status") in ("PAID",):
        return
    if car.get("status") not in ("AT_EXIT", "PAYMENT_PENDING"):
        return

    upsert_car(plate, status="PAYMENT_HOLD")
    log_anomaly(
        plate,
        "PAYMENT_TIMEOUT",
        f"No valid payment after {PAYMENT_TIMEOUT_SEC}s. Exit remains locked; following cars stay parked."
    )
    log_decision(
        plate,
        "PAYMENT_HOLD",
        "Vehicle isolated at exit; automatic release blocked.",
        "Fail-safe mode prevents unpaid escape and prevents other cars from piling into EXIT."
    )

    recovery = threading.Thread(
        target=exit_lane_recovery_watch,
        args=(plate,),
        daemon=True
    )
    recovery.start()


def exit_lane_recovery_watch(plate, hold_seconds=EXIT_HOLD_RECOVERY_SEC):
    """
    Prevent one unresolved payment from blocking the facility forever.

    SAFETY: we do NOT simply clear exit_lane_plate while the unpaid car is
    physically sitting at EXIT. First try to return that car to a safe parking
    spot (normally its original spot). Only after the reroute command succeeds
    do we free the exit lane for the next vehicle.

    If no safe holding spot exists, keep PAYMENT_HOLD and require operator
    intervention rather than creating an exit collision or unpaid escape.
    """
    global exit_lane_plate

    time.sleep(hold_seconds)
    car = get_car(plate)
    if not car:
        return
    if car.get("payment_status") == "PAID":
        return
    if car.get("status") not in ("PAYMENT_HOLD", "PAYMENT_PENDING", "AT_EXIT"):
        return

    preferred = car.get("assigned_spot")
    car_type = car.get("car_type") or "Normal"
    hold_spot = None

    # Prefer the original bay if it is now free and safe.
    if preferred:
        s = spots.get(preferred)
        res = reserved_spots.get(preferred)
        if (s and not s.get("occupied") and not s.get("broken")
                and not s.get("isUnderMaintenance")
                and compatible(s, car_type)
                and (not res or res.get("plate") == plate)):
            hold_spot = preferred

    # Otherwise choose another safe free bay.
    if not hold_spot:
        hold_spot, reason = choose_spot(
            car_type, int(car.get("planned_minutes") or 0)
        )

    if not hold_spot:
        log_decision(
            plate, "EXIT_LANE_ESCALATION",
            "Payment unresolved and no safe holding bay exists.",
            "Exit lane remains blocked; operator intervention required. "
            "PARKMIND refuses to create a collision or unpaid escape."
        )
        return

    try:
        reserved_spots[hold_spot] = {"plate": plate, "reserved_at": time.time()}
        send_car(plate, hold_spot)
        upsert_car(
            plate,
            assigned_spot=hold_spot,
            status="PAYMENT_HOLD_REPARKING"
        )
        append_journey(plate, f"payment_hold_repark->{hold_spot}")
        log_decision(
            plate, "EXIT_LANE_RECOVERY",
            f"Unpaid vehicle rerouted from EXIT to {hold_spot}.",
            "Clears the single exit approach without allowing unpaid departure."
        )

        try:
            close_gate(EXIT_GATE)
        except Exception:
            pass

        with state_lock:
            if exit_lane_plate == plate:
                exit_lane_plate = None

        threading.Timer(1.0, dispatch_next_exit_lane).start()

    except Exception as e:
        reserved_spots.pop(hold_spot, None)
        log_decision(
            plate, "EXIT_LANE_RECOVERY_ERROR", str(e),
            "Lane remains held; operator intervention required."
        )


def process_exit_queue():
    global exit_active
    with state_lock:
        if exit_active is not None or not exit_queue:
            return
        plate = exit_queue.pop(0)
        exit_active = {"plate": plate, "sent": False}
        try:
            opened = open_gate(EXIT_GATE)
            if opened is False:
                raise RuntimeError(f"Exit gate {EXIT_GATE} failed safety/open check")

            watchdog = threading.Timer(
                0.65, exit_gate_open_watchdog, args=(plate, 2)
            )
            watchdog.daemon = True
            watchdog.start()

            release_fallback = threading.Timer(
                1.20, delayed_exit_release, args=(plate,)
            )
            release_fallback.daemon = True
            release_fallback.start()
        except Exception as e:
            log_decision(plate, "ERROR", f"Exit release failed: {e}")


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
            if elapsed < ENTRY_TRANSIT_TIMEOUT_SEC:
                continue

            spot = active["spot"]
            retries = int(active.get("route_retries") or 0)

            car = get_car(plate)
            if car and car.get("status") == "PARKED":
                with state_lock:
                    in_transit.pop(plate, None)
                continue

            if retries < ENTRY_TRANSIT_MAX_RETRIES:
                try:
                    send_car(plate, spot)
                    with state_lock:
                        if plate in in_transit:
                            in_transit[plate]["route_retries"] = retries + 1
                            in_transit[plate]["sent_at"] = time.time()

                    log_decision(
                        plate,
                        "ENTRY_TRANSIT_RETRY",
                        f"Re-sent route to {spot} ({retries + 1}/{ENTRY_TRANSIT_MAX_RETRIES})",
                        f"No Park/CarIn confirmation after {ENTRY_TRANSIT_TIMEOUT_SEC}s."
                    )
                    with state_lock:
                        stats["auto_recoveries"] += 1
                    continue

                except Exception as e:
                    log_decision(
                        plate,
                        "ENTRY_TRANSIT_RETRY_ERROR",
                        str(e),
                        "This vehicle remains independently monitored; entrance flow continues."
                    )
                    with state_lock:
                        if plate in in_transit:
                            in_transit[plate]["sent_at"] = time.time()
                    continue

            # This one vehicle failed to reach its bay after bounded retries.
            try:
                send_car(plate, "leavepark")
                log_decision(
                    plate,
                    "ENTRY_TRANSIT_ABORT",
                    f"Vehicle failed to reach {spot}; redirected out after retries.",
                    "Only this vehicle is isolated; other arrivals continue normally."
                )
            except Exception as e:
                log_decision(
                    plate,
                    "ENTRY_TRANSIT_ABORT_ERROR",
                    str(e),
                    "Vehicle marked for operator recovery without blocking entry."
                )

            with state_lock:
                res = reserved_spots.get(spot)
                if res and res.get("plate") == plate:
                    reserved_spots.pop(spot, None)

                in_transit.pop(plate, None)

                # Only clear entry_active if this same vehicle somehow never generated EntrySpot CarOut.
                if entry_active and entry_active.get("plate") == plate:
                    entry_active = None

            upsert_car(
                plate,
                status="ENTRY_ABORTED",
                decision=f"Did not confirm arrival at {spot} after transit retries."
            )

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


# =====================================================================
# WEBHOOK PROCESSING
# =====================================================================
def handle_event(data):
    global entry_active, exit_active, exit_lane_plate
    event_class = data.get("EventClass")

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

                if name == ENTRY_GATE and action in ("Opening", "Open"):
                    if entry_active and not entry_active["sent"]:
                        plate = entry_active["plate"]
                        spot = entry_active["spot"]
                        try:
                            # Critical ordering: set sent=True only AFTER API success.
                            send_car(plate, spot)
                            mark_entry_dispatched(plate, spot)
                            log_decision(
                                plate, "ENTRY_RELEASE",
                                f"{ENTRY_GATE}={action}; routed to {spot}",
                                "Physical gate movement confirmed before vehicle release."
                            )
                        except Exception as e:
                            log_decision(
                                plate, "ENTRY_RELEASE_ERROR", str(e),
                                "sent flag NOT set; delayed fallback/watchdog can retry."
                            )

                if name == ENTRY_GATE and action == "Closed":
                    if entry_active is None and entry_queue:
                        threading.Timer(0.05, process_entry_queue).start()

                if name == EXIT_GATE and action in ("Opening", "Open"):
                    if exit_active and not exit_active["sent"]:
                        plate = exit_active["plate"]
                        try:
                            # Critical ordering: set sent=True only AFTER API success.
                            send_car(plate, "leavepark")
                            exit_active["sent"] = True
                            log_decision(
                                plate, "EXIT_RELEASE",
                                f"{EXIT_GATE}={action}; releasing paid vehicle",
                                "Physical gate movement confirmed before final departure."
                            )
                        except Exception as e:
                            log_decision(
                                plate, "EXIT_RELEASE_ERROR", str(e),
                                "sent flag NOT set; delayed fallback/watchdog can retry."
                            )

                if name == EXIT_GATE and action == "Closed":
                    if exit_active is None:
                        threading.Thread(target=process_exit_queue, daemon=True).start()

        elif event_class == "car_spot_action":
            plate = data.get("CarPlateNumber")
            car_type = data.get("CarType") or "Normal"
            spot_name = data.get("SpotName")
            spot_type = data.get("SpotType")
            direction = data.get("Direction")
            server_time = data.get("ServerDateTime") or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            planned = int(data.get("PlannedParkingDurationInMinutes") or 0)

            if spot_type == "EntrySpot" and direction == "CarIn":
                with state_lock:
                    stats["total_arrivals"] += 1
                upsert_car(plate, car_type=car_type, planned_minutes=planned,
                           entry_time=server_time, status="WAITING")
                append_journey(plate, "arrived")
                log_decision(plate, "ARRIVAL",
                             f"Arrived at {spot_name}; planned {planned} min; type {car_type}")

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

                with state_lock:
                    if entry_active and entry_active.get("plate") == plate:
                        entry_active = None
                    waiting = bool(entry_queue)

                if waiting:
                    # Next car is already physically waiting at ENTRY1.
                    # Keep gateA open because a car is actively about to enter.
                    threading.Timer(0.05, process_entry_queue).start()
                else:
                    # Nobody is waiting: close gateA immediately.
                    threading.Timer(0.05, close_entry_if_idle).start()

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
                car = get_car(plate)
                if car and car.get("payment_status") in ("REQUESTED", "PAID"):
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

                def delayed_charge():
                    time.sleep(1.2)
                    latest = get_car(plate)
                    if not latest or latest.get("payment_status") != "WAITING_TO_CHARGE":
                        return
                    try:
                        upsert_car(plate, payment_status="REQUESTED",
                                   status="PAYMENT_PENDING")
                        charge_car(plate, parking_cost, charging_cost)
                    except Exception as e:
                        upsert_car(plate, payment_status="CHARGE_ERROR")
                        log_decision(plate, "CHARGE_ERROR", str(e))

                threading.Thread(target=delayed_charge, daemon=True).start()
                threading.Thread(target=payment_timeout_watch, args=(plate,), daemon=True).start()

            elif spot_type == "ExitSpot" and direction == "CarOut":
                # Total stay = ENTRY CarIn -> EXIT CarOut.
                # Store exact seconds; rounded minutes are only a summary.
                total_stay_minutes = 0
                total_stay_seconds = 0
                car_before_departure = get_car(plate)

                # Critical audit condition: a physical departure without an accepted payment.
                if car_before_departure and car_before_departure.get("payment_status") != "PAID":
                    record_alert(
                        alert_key=f"UNPAID_EXIT:{plate}:{data.get('EventId') or data.get('SequenceId') or server_time}",
                        alert_type="ESCAPED WITHOUT PAYMENT",
                        severity="CRITICAL",
                        plate=plate,
                        reason=(
                            f"Vehicle physically left EXIT with payment status "
                            f"{car_before_departure.get('payment_status') or 'NONE'}."
                        ),
                        event_time=server_time
                    )
                total_stay_seconds = seconds_between(
                    (car_before_departure or {}).get("entry_time"),
                    server_time
                )
                if total_stay_seconds > 0:
                    total_stay_minutes = max(
                        1, math.ceil(total_stay_seconds / 60)
                    )
                else:
                    total_stay_minutes = int(
                        (car_before_departure or {}).get("billable_minutes") or 0
                    )

                final_departure_status = (
                    "LEFT"
                    if (car_before_departure or {}).get("payment_status") == "PAID"
                    else "ESCAPED_UNPAID"
                )
                upsert_car(
                    plate,
                    departure_time=server_time,
                    total_stay_minutes=total_stay_minutes,
                    total_stay_seconds=total_stay_seconds,
                    status=final_departure_status
                )
                resolve_exit_stuck_alert(plate)
                append_journey(plate, "departed")
                log_decision(plate, "DEPARTED", "Car left parking")
                with state_lock:
                    stats["total_departed"] += 1
                    car = get_car(plate)
                    if car:
                        stats["total_revenue"] += float(car.get("actual_paid") or 0)
                        spot = car.get("assigned_spot")
                        if spot:
                            # Update spot analytics
                            try:
                                parked = datetime.strptime(car.get("parked_time",""), "%Y-%m-%d %H:%M:%S")
                                exit_t = datetime.strptime(server_time, "%Y-%m-%d %H:%M:%S")
                                dur_min = max(1, int((exit_t - parked).total_seconds()/60))
                            except Exception:
                                dur_min = int(car.get("planned_minutes") or 1)
                            update_spot_stats(spot, dur_min, float(car.get("actual_paid") or 0))

                    if exit_active and exit_active["plate"] == plate:
                        exit_active = None
                        try:
                            close_gate(EXIT_GATE)
                        except Exception as e:
                            log_decision(plate, "ERROR", f"Close exit gate failed: {e}")
                        threading.Timer(1.0, process_exit_queue).start()

                    # The physical exit approach is now clear. Only now may the
                    # next waiting parked car be released toward EXIT.
                    if exit_lane_plate == plate:
                        exit_lane_plate = None
                        threading.Timer(1.0, dispatch_next_exit_lane).start()

        elif event_class == "payment_made":
            plate = data.get("CarPlateNumber")
            received = float(data.get("Amount") or 0)

            ok, factors = verify_payment(plate, received)
            upsert_car(plate, payment_factors=factors)

            if ok:
                upsert_car(plate, payment_status="PAID", status="PAID",
                           actual_paid=received)
                log_decision(plate, "PAYMENT_OK",
                             f"Expected matches received={received}",
                             f"FACTORS: {factors}")
                with state_lock:
                    if plate not in exit_queue and not (exit_active and exit_active["plate"] == plate):
                        exit_queue.append(plate)
                process_exit_queue()
            else:
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
                    "A legitimate late/retry payment is allowed."
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
            name = data.get("Name")
            with state_lock:
                if name in spots:
                    spots[name]["broken"] = True
                if name in gates:
                    gates[name]["broken"] = True
            log_decision("", "COMPONENT_BROKEN", str(name),
                         "Component isolated from automatic selection/control.")

        elif event_class == "component_fixed":
            name = data.get("Name")
            with state_lock:
                if name in spots:
                    spots[name]["broken"] = False
                    spots[name]["isUnderMaintenance"] = False
                if name in gates:
                    gates[name]["broken"] = False
                    gates[name]["isUnderMaintenance"] = False
            log_decision("", "COMPONENT_FIXED", str(name))

        elif event_class == "penalty":
            reason = data.get("Reason")
            fine = data.get("FineAmount")
            penalty_time = data.get("ServerDateTime") or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            penalty_plate = infer_plate_for_alert(
                data=data,
                reason=reason,
                event_time=penalty_time
            )

            # If this is explicitly an unpaid escape, preserve that status.
            lower_reason = str(reason or "").lower()
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
            # Future-ready: log the CO2 level even in Level 1.
            zone = data.get("ZoneName")
            level = data.get("CarbonMonoxideLevel")
            danger = data.get("DangerLevel")
            with state_lock:
                if zone:
                    zones.setdefault(zone, {"name": zone})
                    zones[zone]["gasCarbonMonoxideLevel"] = level
                    zones[zone]["risk"] = danger
            log_decision("", "CO2_EVENT", f"zone={zone} level={level} danger={danger}",
                         "Future-ready: Level 2/3 exhaust fan automation will engage here.")

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

    # Signature integrity handling:
    #   True  = verified
    #   None  = unsigned (accepted for Level 1, clearly labelled)
    #   False = signature mismatch (rejected)
    sig_result = verify_webhook_signature(data)

    if sig_result is False:
        with state_lock:
            stats["webhooks_rejected_sig"] += 1
        log_decision("", "WEBHOOK_REJECTED", "Signature mismatch",
                     "Integrity check failed; event was not acted on.")
        return jsonify({"status": "signature_invalid"}), 200

    if sig_result is True:
        with state_lock:
            stats["webhooks_verified"] += 1
        signature_label = "verified"
    else:
        with state_lock:
            stats["webhooks_unsigned_accepted"] += 1
        signature_label = "unsigned_level1"

    # Sequence gap detection still works for both signed and unsigned events.
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
USERS = {
    "admin": {"password": "admin", "role": "Admin"},
    "operator": {"password": "operator", "role": "Operator"}
}

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
<p class="sub">Pretty Little Hackers — Level 1 Control Center</p>
<div style="margin:10px 0">
<span class="badge">Signature Verified</span>
<span class="badge">Sequence Gap Detection</span>
<span class="badge">Multi-Factor Payment</span>
<span class="badge">Smart Spot AI</span>
</div>
{% if error %}<p class="err">{{error}}</p>{% endif %}
<form method="post">
<input name="username" placeholder="Username" required>
<input name="password" type="password" placeholder="Password" required>
<button>Login</button>
</form>
<p class="sub">admin/admin or operator/operator</p>
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
{% if is_admin %}<a href="/export/cars">Export</a>{% endif %}
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
<div><b>{{g.name}}</b><div class="mini">{{g.state}} · health {{g.health}}% {% if g.broken %}· BROKEN{% endif %}</div></div>
<div>
<form method="post" action="/gate/{{g.name}}/open"><button>Open</button></form>
<form method="post" action="/gate/{{g.name}}/close"><button class="gray">Close</button></form>
</div>
</div>
{% endfor %}
<div class="mini" style="margin-top:7px">Entry: {{entry_gate}} · Exit: {{exit_gate}} · Exit queue: {{exit_waiting}}</div>

<h2 style="margin-top:13px">Zones</h2>
<div class="zones">
{% for z in zone_rows %}
<div class="zone">
<b>{{z.name}}</b>
<div class="mini">{{z.occupied}} occupied · {{z.free}} free · {{z.reserved}} reserved</div>
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
        username = request.form.get("username", "")
        password = request.form.get("password", "")
        u = USERS.get(username)
        if u and u["password"] == password:
            session["user"] = username
            session["role"] = u["role"]
            return redirect(url_for("dashboard"))
        error = "Invalid login"
    return render_template_string(LOGIN_HTML, error=error)


@app.route("/operator/login", methods=["GET", "POST"])
def operator_login():
    error = None
    if request.method == "POST":
        username = request.form.get("username", "operator")
        password = request.form.get("password", "")
        u = USERS.get("operator")
        if username == "operator" and u and u["password"] == password:
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
                "health": component_health_score(name, "spot"),
                "uses": s.get("usage_count", 0),
            })
        gate_rows = [{
            "name": n,
            "state": g.get("state", "?"),
            "broken": g.get("broken", False),
            "health": component_health_score(n, "gate"),
        } for n, g in gates.items()]

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
        for z in sorted(zone_acc):
            item = zone_acc[z]
            item["free"] = max(0, item["total"] - item["occupied"] - item["reserved"])
            item["percent"] = round((item["occupied"] / item["total"]) * 100) if item["total"] else 0
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

    return render_template_string(
        DASH_HTML, user=session["user"], role=session["role"],
        spot_rows=spot_rows, gate_rows=gate_rows, cars=cars,
        alerts=alerts, capacity=capacity,
        decisions=decisions, spot_analytics=spot_analytics,
        stats=snapshot_stats,
        entry_gate=ENTRY_GATE, exit_gate=EXIT_GATE,
        exit_waiting=len(to_exit_queue),
        zone_rows=zone_rows,
        arrival_bars=arrival_bars,
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
    return redirect(url_for("dashboard"))


@app.route("/gate/<name>/<action>", methods=["POST"])
def manual_gate(name, action):
    if not require_login():
        return redirect(url_for("login"))
    try:
        if action == "open":
            open_gate(name)
        elif action == "close":
            close_gate(name)
    except Exception as e:
        log_decision("", "MANUAL_GATE_ERROR", str(e))
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

    try:
        sim_login()
        sync_state()
        if not spots:
            print("[STARTUP] Simulator is connected; Level 1 is not loaded yet.")
            print("[STARTUP] That's OK — the first arrival will trigger a safe sync.")
    except Exception as e:
        print("[STARTUP] Simulator not ready yet:", e)
        print("[STARTUP] Start the simulator; PARKMIND will safely sync on the first arrival.")

    print("\n============================================")
    print("  PARKMIND v2.0 — 'The Wow Factor' Edition")
    print("  Team: Pretty Little Hackers")
    print("  Dashboard: http://127.0.0.1:8000")
    print("  Webhook:   http://127.0.0.1:8000/webhook")
    print("  Login:     admin/admin or operator/operator")
    print("  Entry gate:", ENTRY_GATE, "(verified against /list-barriers)")
    print("  Exit gate: ", EXIT_GATE, "(verified against /list-barriers)")
    print("  Entry control: PIPELINED (next car released on ENTRY1 CarOut, not PARKED)")
    print("  Gate control: DEMAND-DRIVEN (A/B close when no active vehicle needs them)")
    print("  Gate retries: debounced (will not spam OPEN while already Opening/Open)")
    print("============================================\n")

    app.run(host="0.0.0.0", port=WEB_PORT, threaded=True)