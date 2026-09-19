# core/database.py  — all SQLite persistence helpers
import sqlite3
import json
from datetime import datetime
from core.config import DB_PATH
from core import state


def db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
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
        payload TEXT
    );

    CREATE TABLE IF NOT EXISTS cars (
        plate TEXT PRIMARY KEY,
        car_type TEXT,
        planned_minutes INTEGER DEFAULT 0,
        assigned_spot TEXT,
        entry_time TEXT,
        parked_time TEXT,
        exit_arrival_time TEXT,
        departure_time TEXT,
        expected_amount REAL DEFAULT 0,
        billed_minutes INTEGER DEFAULT 0,
        real_duration_seconds REAL DEFAULT 0,
        payment_status TEXT DEFAULT 'NONE',
        status TEXT DEFAULT 'NEW',
        decision TEXT
    );

    CREATE TABLE IF NOT EXISTS decisions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT,
        plate TEXT,
        action TEXT,
        detail TEXT
    );

    CREATE TABLE IF NOT EXISTS zones (
        name TEXT PRIMARY KEY,
        co_risk TEXT DEFAULT 'Safe',
        updated_at TEXT
    );

    CREATE TABLE IF NOT EXISTS fans (
        name TEXT PRIMARY KEY,
        zone_parent TEXT,
        is_on INTEGER DEFAULT 0,
        broken INTEGER DEFAULT 0,
        updated_at TEXT
    );

    CREATE TABLE IF NOT EXISTS penalties (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        received_at TEXT,
        plate TEXT,
        reason TEXT,
        fine_amount REAL DEFAULT 0
    );
    """)
    
    # Try adding new columns to an existing database
    try:
        conn.execute("ALTER TABLE cars ADD COLUMN billed_minutes INTEGER DEFAULT 0")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE cars ADD COLUMN real_duration_seconds REAL DEFAULT 0")
    except Exception:
        pass
        
    conn.commit()
    conn.close()


def load_state_from_db():
    """On startup: reload zones and fans from DB into in-memory dicts."""
    conn = db()
    for row in conn.execute("SELECT * FROM zones").fetchall():
        state.zones[row["name"]] = dict(row)
    for row in conn.execute("SELECT * FROM fans").fetchall():
        state.fans[row["name"]] = dict(row)
    conn.close()
    print(f"[DB] Loaded {len(state.zones)} zones, {len(state.fans)} fans from DB.")


# ------------------------------------------------------------------
# Decision / Event log
# ------------------------------------------------------------------

def log_decision(plate, action, detail):
    conn = db()
    conn.execute(
        "INSERT INTO decisions(created_at, plate, action, detail) VALUES(?,?,?,?)",
        (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), plate, action, detail)
    )
    conn.commit()
    conn.close()
    print(f"[DECISION] {plate or '-'} | {action} | {detail}")


def save_event(data):
    """Insert raw event for deduplication. Returns False if duplicate."""
    event_id = str(data.get("EventId") or "")
    conn = db()
    try:
        conn.execute(
            """INSERT INTO events(event_id, sequence_id, event_class, server_time, payload)
               VALUES(?,?,?,?,?)""",
            (
                event_id,
                data.get("SequenceId"),
                data.get("EventClass"),
                data.get("ServerDateTime"),
                json.dumps(data)
            )
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()


# ------------------------------------------------------------------
# Cars
# ------------------------------------------------------------------

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


# ------------------------------------------------------------------
# Zones (CO monitoring) — write-through cache
# ------------------------------------------------------------------

def upsert_zone(name, co_risk):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = db()
    conn.execute(
        "INSERT INTO zones(name, co_risk, updated_at) VALUES(?,?,?) "
        "ON CONFLICT(name) DO UPDATE SET co_risk=excluded.co_risk, updated_at=excluded.updated_at",
        (name, co_risk, now)
    )
    conn.commit()
    conn.close()
    with state.state_lock:
        if name not in state.zones:
            state.zones[name] = {}
        state.zones[name]["co_risk"] = co_risk
        state.zones[name]["updated_at"] = now


# ------------------------------------------------------------------
# Fans — write-through cache
# ------------------------------------------------------------------

def upsert_fan(name, zone_parent=None, is_on=None, broken=None):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = db()
    conn.execute(
        "INSERT OR IGNORE INTO fans(name, zone_parent, updated_at) VALUES(?,?,?)",
        (name, zone_parent or "", now)
    )
    updates = {"updated_at": now}
    if zone_parent is not None:
        updates["zone_parent"] = zone_parent
    if is_on is not None:
        updates["is_on"] = 1 if is_on else 0
    if broken is not None:
        updates["broken"] = 1 if broken else 0
    cols = ", ".join(f"{k}=?" for k in updates)
    vals = list(updates.values()) + [name]
    conn.execute(f"UPDATE fans SET {cols} WHERE name=?", vals)
    conn.commit()
    conn.close()
    with state.state_lock:
        if name not in state.fans:
            state.fans[name] = {"name": name}
        if zone_parent is not None:
            state.fans[name]["zone_parent"] = zone_parent
        if is_on is not None:
            state.fans[name]["is_on"] = is_on
        if broken is not None:
            state.fans[name]["broken"] = broken


# ------------------------------------------------------------------
# Penalties
# ------------------------------------------------------------------

def log_penalty(reason, fine_amount, plate=""):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = db()
    conn.execute(
        "INSERT INTO penalties(received_at, plate, reason, fine_amount) VALUES(?,?,?,?)",
        (now, plate or "", reason or "", float(fine_amount or 0))
    )
    conn.commit()
    conn.close()
    print(f"[PENALTY] {reason} | fine={fine_amount} | plate={plate}")


# ------------------------------------------------------------------
# Dashboard query helpers
# ------------------------------------------------------------------

def get_recent_cars(limit=50):
    conn = db()
    rows = [dict(r) for r in conn.execute(
        "SELECT * FROM cars ORDER BY COALESCE(entry_time,'') DESC LIMIT ?", (limit,)
    ).fetchall()]
    conn.close()
    return rows


def get_recent_decisions(limit=30):
    conn = db()
    rows = [dict(r) for r in conn.execute(
        "SELECT * FROM decisions ORDER BY id DESC LIMIT ?", (limit,)
    ).fetchall()]
    conn.close()
    return rows


def get_total_penalties():
    conn = db()
    row = conn.execute("SELECT COALESCE(SUM(fine_amount),0) as total FROM penalties").fetchone()
    conn.close()
    return row["total"] if row else 0
