from flask import Flask, request, jsonify, render_template_string, redirect, url_for, session
import requests
import sqlite3
import threading
import json
import math
import re
from datetime import datetime
from urllib.parse import quote

# -----------------------------
# CONFIG
# -----------------------------
SIM_BASE = "http://127.0.0.1:9898/api/v1"
SIM_USER = "admin"
SIM_PASSWORD = "admin"

ENTRY_GATE = "gateA"
EXIT_GATE = "gateB"   # TEST THIS. If gateB is not the physical exit gate, change to gateC.

WEB_PORT = 8000

app = Flask(__name__)
app.secret_key = "pretty-little-hackers-level1"

token = None
spots = {}   # name -> spot dict
gates = {}   # name -> gate dict
fans = {}    # name -> fan dict  (write-through cached)
zones = {}   # name -> {co_risk, ...} (write-through cached)
reserved_spots = set()

entry_queue = []
entry_active = None

exit_queue = []
exit_active = None

state_lock = threading.RLock()

# -----------------------------
# DATABASE
# -----------------------------
def db():
    conn = sqlite3.connect("parkmind.db", timeout=10)
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
    conn.commit()
    conn.close()

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

def upsert_zone(name, co_risk):
    """Persist CO zone risk level to DB and update in-memory cache."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = db()
    conn.execute(
        "INSERT INTO zones(name, co_risk, updated_at) VALUES(?,?,?) "
        "ON CONFLICT(name) DO UPDATE SET co_risk=excluded.co_risk, updated_at=excluded.updated_at",
        (name, co_risk, now)
    )
    conn.commit()
    conn.close()
    with state_lock:
        if name not in zones:
            zones[name] = {}
        zones[name]["co_risk"] = co_risk
        zones[name]["updated_at"] = now

def upsert_fan(name, zone_parent=None, is_on=None, broken=None):
    """Persist fan state to DB and update in-memory cache."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = db()
    # Ensure row exists
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
    with state_lock:
        if name not in fans:
            fans[name] = {"name": name}
        if zone_parent is not None:
            fans[name]["zone_parent"] = zone_parent
        if is_on is not None:
            fans[name]["is_on"] = is_on
        if broken is not None:
            fans[name]["broken"] = broken

def log_penalty(reason, fine_amount, plate=""):
    """Persist penalty to DB for tracking score."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = db()
    conn.execute(
        "INSERT INTO penalties(received_at, plate, reason, fine_amount) VALUES(?,?,?,?)",
        (now, plate or "", reason or "", float(fine_amount or 0))
    )
    conn.commit()
    conn.close()
    print(f"[PENALTY] {reason} | fine={fine_amount} | plate={plate}")

def load_state_from_db():
    """On startup: reload zones and fans from DB into in-memory dicts."""
    conn = db()
    for row in conn.execute("SELECT * FROM zones").fetchall():
        zones[row["name"]] = dict(row)
    for row in conn.execute("SELECT * FROM fans").fetchall():
        fans[row["name"]] = dict(row)
    conn.close()
    print(f"[DB] Loaded {len(zones)} zones, {len(fans)} fans from DB.")

# -----------------------------
# SIMULATOR REST API
# -----------------------------
def sim_login():
    global token
    r = requests.post(
        f"{SIM_BASE}/auth/login",
        json={"Email": SIM_USER, "Password": SIM_PASSWORD},
        timeout=5
    )
    r.raise_for_status()
    token = r.json()["token"]
    print("[API] Logged in to simulator.")

def sim_request(method, path, **kwargs):
    global token
    if not token:
        sim_login()

    headers = kwargs.pop("headers", {})
    headers["Authorization"] = f"Bearer {token}"

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
        r = requests.request(
            method,
            f"{SIM_BASE}{path}",
            headers=headers,
            timeout=8,
            **kwargs
        )

    if r.status_code >= 400:
        print(f"[API ERROR] {method} {path} -> {r.status_code} {r.text}")
        r.raise_for_status()

    return r

def detected_count(value):
    if isinstance(value, int):
        return value
    if isinstance(value, list):
        return len(value)
    try:
        return int(value or 0)
    except Exception:
        return 0

def sync_state():
    with state_lock:
        park_data = sim_request("GET", "/list-parking-spots").json()
        barrier_data = sim_request("GET", "/list-barriers").json()

        spots.clear()
        for s in park_data:
            if s.get("purpose") == "Park":
                spots[s["name"]] = {
                    **s,
                    "occupied": detected_count(s.get("detectedCars")) > 0
                }

        gates.clear()
        for g in barrier_data:
            gates[g["name"]] = dict(g)

        # Sync fans from simulator
        try:
            fan_data = sim_request("GET", "/list-exhaust-fans").json()
            for f in fan_data:
                upsert_fan(
                    f["name"],
                    zone_parent=f.get("zoneParent", ""),
                    is_on=f.get("isOn", False),
                    broken=f.get("broken", False)
                )
        except Exception as e:
            print(f"[SYNC] Could not load fans: {e}")

        # Sync zones from simulator
        try:
            zone_data = sim_request("GET", "/list-zones").json()
            for z in zone_data:
                upsert_zone(z["name"], z.get("risk", "Safe"))
        except Exception as e:
            print(f"[SYNC] Could not load zones: {e}")

    print(f"[SYNC] {len(spots)} spots, {len(gates)} gates, {len(fans)} fans, {len(zones)} zones.")
    log_decision("", "SYNC", f"Loaded {len(spots)} spots, {len(gates)} gates, {len(fans)} fans, {len(zones)} zones")

def gate_safe(name):
    g = gates.get(name)
    if not g:
        return True
    return not g.get("broken", False) and not g.get("isUnderMaintenance", False)

def open_gate(name):
    if not gate_safe(name):
        log_decision("", "BLOCKED", f"Refused to open {name}: broken/maintenance")
        return False
    sim_request("POST", f"/barrier-gates/{quote(name, safe='')}/open")
    log_decision("", "GATE_OPEN", name)
    return True

def close_gate(name):
    if not gate_safe(name):
        log_decision("", "BLOCKED", f"Refused to close {name}: broken/maintenance")
        return False
    sim_request("POST", f"/barrier-gates/{quote(name, safe='')}/close")
    log_decision("", "GATE_CLOSE", name)
    return True

def send_car(plate, destination):
    plate_path = quote(plate.replace(" ", ""), safe="")
    dest_path = quote(destination, safe="")
    sim_request("POST", f"/car/{plate_path}/goto/{dest_path}")
    log_decision(plate, "CAR_GOTO", destination)

def charge_car(plate, parking_cost, charging_cost):
    plate_path = quote(plate.replace(" ", ""), safe="")
    sim_request(
        "POST",
        f"/car/{plate_path}/charge",
        params={
            "parkingCost": parking_cost,
            "chargingCost": charging_cost
        }
    )
    log_decision(plate, "CHARGE", f"parking={parking_cost}, charging={charging_cost}")

# -----------------------------
# PARKING LOGIC
# -----------------------------
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

def choose_spot(car_type):
    candidates = []
    for name, s in spots.items():
        if s.get("occupied") or s.get("broken") or s.get("isUnderMaintenance"):
            continue
        if name in reserved_spots or not compatible(s, car_type):
            continue
        candidates.append(name)
    candidates.sort(key=natural_spot_key)
    return candidates[0] if candidates else None

def process_entry_queue():
    global entry_active
    with state_lock:
        if entry_active is not None or not entry_queue:
            return

        if not spots:
            try:
                sync_state()
            except Exception as e:
                print("[SYNC ERROR]", e)
                return

        item = entry_queue.pop(0)
        plate = item["plate"]
        car_type = item["car_type"]
        spot = choose_spot(car_type)

        if not spot:
            upsert_car(plate, status="LEFT_FULL")
            log_decision(plate, "NO_SPACE", "No safe compatible parking spot. Sending car away.")
            try:
                send_car(plate, "leavepark")
            except Exception as e:
                log_decision(plate, "ERROR", f"leavepark failed: {e}")
            threading.Timer(0.2, process_entry_queue).start()
            return

        reserved_spots.add(spot)
        entry_active = {"plate": plate, "spot": spot, "sent": False}
        reason = f"{spot} selected: FREE + HEALTHY + COMPATIBLE"
        upsert_car(plate, assigned_spot=spot, status="ASSIGNED", decision=reason)
        log_decision(plate, "ASSIGN", reason)

        gate_state = gates.get(ENTRY_GATE, {}).get("state")
        try:
            if gate_state == "Open":
                entry_active["sent"] = True
                send_car(plate, spot)
            else:
                open_gate(ENTRY_GATE)
        except Exception as e:
            log_decision(plate, "ERROR", f"Entry handling failed: {e}")

def schedule_exit(plate, planned_minutes):
    seconds = max(1, int(planned_minutes)) * 60
    log_decision(plate, "TIMER", f"Exit scheduled in {seconds} seconds")
    timer = threading.Timer(seconds, send_to_exit, args=(plate,))
    timer.daemon = True
    timer.start()

def send_to_exit(plate):
    car = get_car(plate)
    if not car or car["status"] not in ("PARKED", "ASSIGNED"):
        return
    try:
        send_car(plate, "exit")
        upsert_car(plate, status="TO_EXIT")
    except Exception as e:
        log_decision(plate, "ERROR", f"Could not send to exit: {e}")

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

    parking_cost = float(minutes)
    charging_cost = float(minutes) if "electric" in str(car.get("car_type", "")).lower() else 0.0
    return minutes, parking_cost, charging_cost

def process_exit_queue():
    global exit_active
    with state_lock:
        if exit_active is not None or not exit_queue:
            return

        plate = exit_queue.pop(0)
        exit_active = {"plate": plate, "sent": False}
        gate_state = gates.get(EXIT_GATE, {}).get("state")

        try:
            if gate_state == "Open":
                exit_active["sent"] = True
                send_car(plate, "leavepark")
            else:
                open_gate(EXIT_GATE)
        except Exception as e:
            log_decision(plate, "ERROR", f"Exit release failed: {e}")

# -----------------------------
# WEBHOOK PROCESSING
# -----------------------------
def handle_event(data):
    global entry_active, exit_active
    event_class = data.get("EventClass")

    try:
        if event_class == "gate_action":
            name = data.get("Name")
            action = data.get("Action")

            with state_lock:
                if name not in gates:
                    gates[name] = {"name": name}
                gates[name]["state"] = action

                if name == ENTRY_GATE and action == "Open":
                    if entry_active and not entry_active["sent"]:
                        entry_active["sent"] = True
                        send_car(entry_active["plate"], entry_active["spot"])

                if name == ENTRY_GATE and action == "Closed":
                    if entry_active is None:
                        threading.Thread(target=process_entry_queue, daemon=True).start()

                if name == EXIT_GATE and action == "Open":
                    if exit_active and not exit_active["sent"]:
                        exit_active["sent"] = True
                        send_car(exit_active["plate"], "leavepark")

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
                upsert_car(
                    plate, car_type=car_type, planned_minutes=planned,
                    entry_time=server_time, status="WAITING"
                )
                log_decision(plate, "ARRIVAL", f"Arrived at {spot_name}; planned {planned} min")

                with state_lock:
                    already_queued = any(x["plate"] == plate for x in entry_queue)
                    already_active = entry_active and entry_active["plate"] == plate
                    if not already_queued and not already_active:
                        entry_queue.append({"plate": plate, "car_type": car_type, "planned": planned})
                process_entry_queue()

            elif spot_type == "Park":
                with state_lock:
                    if spot_name in spots:
                        spots[spot_name]["occupied"] = (direction == "CarIn")

                if direction == "CarIn":
                    reserved_spots.discard(spot_name)
                    upsert_car(plate, assigned_spot=spot_name, parked_time=server_time, status="PARKED")
                    log_decision(plate, "PARKED", spot_name)

                    planned_minutes = get_car(plate).get("planned_minutes") or planned or 1
                    schedule_exit(plate, planned_minutes)

                    with state_lock:
                        if entry_active and entry_active["plate"] == plate:
                            entry_active = None
                            try:
                                close_gate(ENTRY_GATE)
                            except Exception as e:
                                log_decision(plate, "ERROR", f"Close entry gate failed: {e}")
                            threading.Timer(1.0, process_entry_queue).start()

                elif direction == "CarOut":
                    log_decision(plate, "LEFT_SPOT", spot_name)

            elif spot_type == "ExitSpot" and direction == "CarIn":
                car = get_car(plate)
                if car and car.get("payment_status") == "REQUESTED":
                    return

                minutes, parking_cost, charging_cost = calculate_charge(plate, server_time)
                expected = parking_cost + charging_cost

                upsert_car(
                    plate, exit_arrival_time=server_time, expected_amount=expected,
                    payment_status="REQUESTED", status="PAYMENT_PENDING"
                )
                log_decision(plate, "AT_EXIT", f"{minutes} min; expected total={expected}")
                charge_car(plate, parking_cost, charging_cost)

            elif spot_type == "ExitSpot" and direction == "CarOut":
                upsert_car(plate, departure_time=server_time, status="LEFT")
                log_decision(plate, "DEPARTED", "Car left parking")

                with state_lock:
                    if exit_active and exit_active["plate"] == plate:
                        exit_active = None
                        try:
                            close_gate(EXIT_GATE)
                        except Exception as e:
                            log_decision(plate, "ERROR", f"Close exit gate failed: {e}")
                        threading.Timer(1.0, process_exit_queue).start()

        elif event_class == "payment_made":
            plate = data.get("CarPlateNumber")
            received = float(data.get("Amount") or 0)
            car = get_car(plate)

            if not car:
                log_decision(plate, "PAYMENT_REJECTED", "Unknown car")
                return

            expected = float(car.get("expected_amount") or 0)

            if abs(received - expected) < 0.001:
                upsert_car(plate, payment_status="PAID", status="PAID")
                log_decision(plate, "PAYMENT_OK", f"Expected {expected}, received {received}")

                with state_lock:
                    if plate not in exit_queue and not (exit_active and exit_active["plate"] == plate):
                        exit_queue.append(plate)
                process_exit_queue()
            else:
                upsert_car(plate, payment_status="INVALID")
                log_decision(
                    plate, "PAYMENT_REJECTED",
                    f"Expected {expected}, received {received}. Gate remains closed."
                )

        elif event_class == "component_broken":
            name = data.get("Name")
            with state_lock:
                if name in spots:
                    spots[name]["broken"] = True
                if name in gates:
                    gates[name]["broken"] = True
                if name in fans:
                    fans[name]["broken"] = True
            # Persist to DB
            if name in fans:
                upsert_fan(name, broken=True)
            log_decision("", "COMPONENT_BROKEN", str(name))
            # Auto-repair immediately
            try:
                comp_type = data.get("ComponentType", "").lower()
                if name in gates or "gate" in comp_type or "barrier" in comp_type:
                    sim_request("POST", f"/barrier-gates/{quote(name, safe='')}/repair")
                    log_decision("", "AUTO_REPAIR", f"Gate {name} repair triggered")
                elif name in fans or "fan" in comp_type:
                    sim_request("POST", f"/exhaust-fans/{quote(name, safe='')}/repair")
                    log_decision("", "AUTO_REPAIR", f"Fan {name} repair triggered")
                elif name in spots or "spot" in comp_type:
                    sim_request("POST", f"/parking-spots/{quote(name, safe='')}/repair")
                    log_decision("", "AUTO_REPAIR", f"Spot {name} repair triggered")
                else:
                    # Try all repair endpoints
                    for endpoint in [
                        f"/barrier-gates/{quote(name, safe='')}/repair",
                        f"/exhaust-fans/{quote(name, safe='')}/repair",
                        f"/parking-spots/{quote(name, safe='')}/repair",
                    ]:
                        try:
                            sim_request("POST", endpoint)
                            log_decision("", "AUTO_REPAIR", f"{name} repaired via {endpoint}")
                            break
                        except Exception:
                            pass
            except Exception as e:
                log_decision("", "REPAIR_ERROR", f"{name}: {e}")

        elif event_class == "component_fixed":
            name = data.get("Name")
            with state_lock:
                if name in spots:
                    spots[name]["broken"] = False
                if name in gates:
                    gates[name]["broken"] = False
                if name in fans:
                    fans[name]["broken"] = False
            if name in fans:
                upsert_fan(name, broken=False)
            log_decision("", "COMPONENT_FIXED", str(name))

        elif event_class in ("zone_status", "co_alert"):
            # CO gas monitoring — auto-control fans
            zone_name = data.get("ZoneName") or data.get("Name")
            risk = data.get("Risk") or data.get("CoRisk", "Safe")
            if zone_name:
                upsert_zone(zone_name, risk)
                log_decision("", "ZONE_CO", f"{zone_name} -> {risk}")

            if risk in ("High", "Moderate"):
                # Turn on all fans in this zone
                with state_lock:
                    zone_fans = [name for name, f in fans.items()
                                 if f.get("zone_parent") == zone_name and not f.get("broken")]
                for fan_name in zone_fans:
                    try:
                        sim_request("POST", f"/exhaust-fans/{quote(fan_name, safe='')}/on")
                        upsert_fan(fan_name, is_on=True)
                        log_decision("", "FAN_ON", f"{fan_name} activated due to {risk} CO in {zone_name}")
                    except Exception as e:
                        log_decision("", "FAN_ERROR", f"{fan_name}: {e}")
            elif risk == "Safe":
                # Turn off fans when safe
                with state_lock:
                    zone_fans = [name for name, f in fans.items()
                                 if f.get("zone_parent") == zone_name
                                 and f.get("is_on") and not f.get("broken")]
                for fan_name in zone_fans:
                    try:
                        sim_request("POST", f"/exhaust-fans/{quote(fan_name, safe='')}/off")
                        upsert_fan(fan_name, is_on=False)
                        log_decision("", "FAN_OFF", f"{fan_name} deactivated, zone {zone_name} safe")
                    except Exception as e:
                        log_decision("", "FAN_ERROR", f"{fan_name}: {e}")

        elif event_class == "penalty":
            reason = data.get("Reason", "Unknown")
            fine = data.get("FineAmount", 0)
            plate = data.get("CarPlateNumber", "")
            log_penalty(reason, fine, plate)
            log_decision(plate, "PENALTY", f"{reason} | fine={fine}")

    except Exception as e:
        print("[EVENT ERROR]", e)
        log_decision("", "ERROR", f"{event_class}: {e}")

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(silent=True) or {}
    print("\n[WEBHOOK]", data)

    if data.get("EventId") and not save_event(data):
        print("[WEBHOOK] Duplicate EventId ignored.")
        return jsonify({"status": "duplicate_ignored"}), 200

    threading.Thread(target=handle_event, args=(data,), daemon=True).start()
    return jsonify({"status": "received"}), 200

# -----------------------------
# WEB DASHBOARD + ROLES
# -----------------------------
USERS = {
    "admin": {"password": "admin", "role": "Admin"},
    "operator": {"password": "operator", "role": "Operator"}
}

LOGIN_HTML = """
<!doctype html>
<title>PARKMIND Login</title>
<style>
body{font-family:Arial;background:#111827;color:white;display:grid;place-items:center;height:100vh;margin:0}
.card{background:#1f2937;padding:32px;border-radius:18px;width:320px}
input,button{width:100%;box-sizing:border-box;padding:12px;margin:8px 0;border-radius:8px;border:0}
button{font-weight:bold;cursor:pointer}.err{color:#fca5a5}
</style>
<div class="card">
<h1>PARKMIND</h1><p>Level 1 Control Center</p>
{% if error %}<p class="err">{{error}}</p>{% endif %}
<form method="post">
<input name="username" placeholder="Username" required>
<input name="password" type="password" placeholder="Password" required>
<button>Login</button>
</form>
<p style="opacity:.7">admin/admin or operator/operator</p>
</div>
"""

DASH_HTML = """
<!doctype html>
<html><head><meta http-equiv="refresh" content="2"><title>PARKMIND</title>
<style>
body{font-family:Arial;margin:0;background:#0f172a;color:#e5e7eb}
header{padding:18px 24px;background:#111827;display:flex;justify-content:space-between;align-items:center}
.wrap{padding:18px;display:grid;grid-template-columns:1.2fr .8fr;gap:16px}
.card{background:#1e293b;border-radius:14px;padding:16px}
.stat{display:inline-block;background:#111827;border-radius:10px;padding:10px 14px;margin:4px}
table{width:100%;border-collapse:collapse;font-size:14px}
th,td{padding:8px;border-bottom:1px solid #334155;text-align:left}
.free{color:#86efac}.busy{color:#fca5a5}.warn{color:#fde68a}
button{padding:7px 10px;border:0;border-radius:8px;cursor:pointer}form{display:inline}
.small{font-size:12px;opacity:.8}.ok{color:#86efac}.bad{color:#fca5a5}
</style></head><body>
<header><div><b>PARKMIND</b> — Pretty Little Hackers</div>
<div>{{role}} | {{user}} | <a style="color:white" href="/logout">Logout</a></div></header>

<div style="padding:14px 18px">
<span class="stat">FREE: <b>{{free_count}}</b></span>
<span class="stat">OCCUPIED/RESERVED: <b>{{busy_count}}</b></span>
<span class="stat">CARS TRACKED: <b>{{cars|length}}</b></span>
<form method="post" action="/sync"><button>SYNC FROM SIMULATOR</button></form>
</div>

<div class="wrap">
<div class="card"><h2>Parking Spots — ZONE1</h2><table>
<tr><th>Spot</th><th>Status</th><th>Type</th><th>Health</th></tr>
{% for s in spot_rows %}
<tr><td>{{s.name}}</td><td class="{{'busy' if s.busy else 'free'}}">{{'OCCUPIED/RESERVED' if s.busy else 'FREE'}}</td>
<td>{{s.type}}</td><td class="{{'warn' if not s.healthy else 'ok'}}">{{'HEALTHY' if s.healthy else 'UNAVAILABLE'}}</td></tr>
{% endfor %}</table></div>

<div class="card"><h2>Barrier Gates</h2>
{% for g in gate_rows %}
<p><b>{{g.name}}</b> — {{g.state}}
{% if g.broken %}<span class="bad">BROKEN</span>{% endif %}
<form method="post" action="/gate/{{g.name}}/open"><button>Open</button></form>
<form method="post" action="/gate/{{g.name}}/close"><button>Close</button></form></p>
{% endfor %}
<p class="small">Configured Entry={{entry_gate}} | Exit={{exit_gate}}</p></div>

<div class="card"><h2>Cars</h2><table>
<tr><th>Plate</th><th>Status</th><th>Spot</th><th>Payment</th><th>Decision</th><th>Test</th></tr>
{% for c in cars %}
<tr><td>{{c.plate}}</td><td>{{c.status}}</td><td>{{c.assigned_spot or '-'}}</td>
<td>{{c.payment_status}} / {{c.expected_amount}}</td><td class="small">{{c.decision or '-'}}</td>
<td>{% if c.status == 'PARKED' %}<form method="post" action="/car/{{c.plate}}/exit"><button>Send to Exit</button></form>{% endif %}</td></tr>
{% endfor %}</table></div>

<div class="card"><h2>Decision Timeline</h2>
{% for d in decisions %}<p class="small"><b>{{d.created_at}}</b> {{d.plate}} — {{d.action}} — {{d.detail}}</p>{% endfor %}
</div></div></body></html>
"""

def require_login():
    return bool(session.get("user"))

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
            busy = bool(s.get("occupied")) or name in reserved_spots
            spot_rows.append({
                "name": name,
                "busy": busy,
                "type": s.get("parkingForCarType", "Any"),
                "healthy": not s.get("broken") and not s.get("isUnderMaintenance")
            })

        gate_rows = [{
            "name": name,
            "state": g.get("state", "?"),
            "broken": g.get("broken", False)
        } for name, g in gates.items()]

    conn = db()
    cars = [dict(r) for r in conn.execute(
        "SELECT * FROM cars ORDER BY COALESCE(entry_time,'') DESC LIMIT 50"
    ).fetchall()]
    decisions = [dict(r) for r in conn.execute(
        "SELECT * FROM decisions ORDER BY id DESC LIMIT 30"
    ).fetchall()]
    conn.close()

    free_count = sum(1 for s in spot_rows if not s["busy"] and s["healthy"])
    busy_count = len(spot_rows) - free_count

    return render_template_string(
        DASH_HTML, user=session["user"], role=session["role"],
        spot_rows=spot_rows, gate_rows=gate_rows, cars=cars, decisions=decisions,
        free_count=free_count, busy_count=busy_count,
        entry_gate=ENTRY_GATE, exit_gate=EXIT_GATE
    )

@app.route("/sync", methods=["POST"])
def sync_route():
    if not require_login():
        return redirect(url_for("login"))
    try:
        sync_state()
    except Exception as e:
        log_decision("", "SYNC_ERROR", str(e))
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
    threading.Thread(target=send_to_exit, args=(plate,), daemon=True).start()
    return redirect(url_for("dashboard"))

if __name__ == "__main__":
    init_db()
    load_state_from_db()  # Restore zones/fans from DB on startup

    try:
        sim_login()
        sync_state()
    except Exception as e:
        print("[STARTUP] Simulator sync not ready yet:", e)
        print("[STARTUP] This is OK. Load Level 1, then click SYNC on the dashboard.")

    print("\n============================================")
    print(" PARKMIND LEVEL 1")
    print(" Dashboard: http://127.0.0.1:8000")
    print(" Webhook:   http://127.0.0.1:8000/webhook")
    print(" Login:     admin/admin or operator/operator")
    print(f" Entry gate: {ENTRY_GATE}")
    print(f" Exit gate:  {EXIT_GATE}  <-- verify physically")
    print("============================================\n")

    app.run(host="0.0.0.0", port=WEB_PORT, threaded=True)
