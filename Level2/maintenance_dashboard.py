from flask import (
    Blueprint, render_template_string, session, redirect,
    flash
)
import sqlite3
from pathlib import Path
import os
import json
import requests
from datetime import datetime
from urllib.parse import quote

# ============================================================
# PARKMIND LEVEL 2 — MAINTENANCE DASHBOARD (SELF-CONTAINED)
# ============================================================
# This file is deliberately compatible with older/newer shared PARKMIND cores.
# It DOES NOT require a "maintenance_required" column in the shared components table.
#
# SOURCE OF TRUTH:
# - Predictive maintenance: GET /api/v1/list-alarms
# - Spot/gate/fan broken + maintenance state: simulator list APIs
# - Occupancy: simulator detectedCars
# - CO safety: GET /api/v1/list-zones
# - Repair completion: signed component_fixed webhook already stored by PARKMIND
# - Penalties: real simulator penalty rows already stored by PARKMIND
#
# NO FAKE:
# - No life %
# - No remaining cycles
# - No invented maintenance threshold
# - No invented penalties
# - No invented repair completion
# ============================================================

maintenance_bp = Blueprint("maintenance", __name__, url_prefix="/maintenance")

APP_DIR = Path(__file__).resolve().parent
DB_PATH = Path(
    os.getenv("PARKMIND_LEVEL2_DB", str(APP_DIR / "parkmind_level2.db"))
)

SIM_BASE = os.getenv("PARKMIND_SIM_BASE", "http://127.0.0.1:9898/api/v1")
SIM_USER = os.getenv("PARKMIND_SIM_USER", "admin")
SIM_PASSWORD = os.getenv("PARKMIND_SIM_PASSWORD", "admin")

TOKEN = None

HTML = """
<!doctype html>
<html>
<head>
<title>PARKMIND — Maintenance</title>
<style>
*{box-sizing:border-box}
body{margin:0;background:#07111f;color:#e5e7eb;font-family:'Segoe UI',Arial,sans-serif}
header{
  min-height:66px;padding:10px 20px;background:#0e1a2c;border-bottom:1px solid #26364e;
  display:flex;align-items:center;justify-content:space-between;gap:12px
}
.brand{font-size:21px;font-weight:900;color:#67e8f9}
.sub,.mini{font-size:10px;color:#94a3b8}
a{color:#67e8f9;text-decoration:none}
.page{max-width:1550px;margin:auto;padding:14px}
.toolbar{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
button{padding:7px 9px;border:0;border-radius:7px;background:#0284c7;color:#fff;font-size:10px;font-weight:700;cursor:pointer}
button.gray{background:#475569} button.good{background:#047857} button.danger{background:#b91c1c}
form{display:inline}
.card,.kpi{background:#101b2d;border:1px solid #26364e;border-radius:13px;padding:12px}
.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(145px,1fr));gap:9px;margin-bottom:10px}
.kpi .v{font-size:23px;font-weight:900}
.kpi .k{font-size:10px;color:#94a3b8;text-transform:uppercase;letter-spacing:.4px}
.grid{display:grid;grid-template-columns:1.35fr .65fr;gap:10px;margin-top:10px}
.two{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:10px}
h2{margin:0 0 9px;font-size:13px;text-transform:uppercase;color:#cbd5e1;letter-spacing:.6px}
.good{color:#86efac}.warn{color:#fbbf24}.bad{color:#f87171}.cyan{color:#67e8f9}
.banner{padding:10px 12px;border-radius:10px;font-size:10px;line-height:1.5;margin-bottom:10px}
.banner.info{background:#082f49;border:1px solid #0e7490;color:#bae6fd}
.banner.bad{background:#281116;border:1px solid #7f1d1d;color:#fecaca}
.banner.good{background:#0d2c23;border:1px solid #047857;color:#bbf7d0}
.flash{padding:9px 11px;border-radius:8px;margin-bottom:8px;background:#2a210d;border:1px solid #92400e;color:#fde68a;font-size:10px}
table{width:100%;border-collapse:collapse;font-size:10px}
th,td{padding:7px;border-bottom:1px solid #26364e;text-align:left;vertical-align:top}
th{font-size:9px;color:#94a3b8;text-transform:uppercase}
.scroll{max-height:430px;overflow:auto}
.badge{display:inline-block;padding:3px 6px;border-radius:999px;background:#1e293b;font-size:9px;font-weight:800}
.badge.bad{background:#3a171b;color:#fca5a5}.badge.warn{background:#3b2a10;color:#fde68a}
.badge.good{background:#0f2f25;color:#86efac}.badge.cyan{background:#082f49;color:#7dd3fc}
.brokenrow{background:#281116}.duerow{background:#2a210d}.repairrow{background:#0a2030}
.reason{max-width:350px;white-space:normal;line-height:1.35}
.empty{padding:18px;text-align:center;color:#64748b}
.zone{padding:9px;background:#0a1424;border:1px solid #26364e;border-radius:9px;margin-bottom:7px}
.zone.danger{background:#281116;border-color:#7f1d1d}
.statline{display:flex;justify-content:space-between;gap:8px;padding:7px 0;border-bottom:1px solid #26364e;font-size:10px}
details{margin-top:10px;background:#0b1424;border:1px solid #26364e;border-radius:10px;padding:9px}
summary{cursor:pointer;font-weight:700;color:#cbd5e1}
@media(max-width:1000px){.grid,.two{grid-template-columns:1fr}}
</style>
</head>
<body>

<header>
<div>
  <div class="brand">PARKMIND · Maintenance</div>
  <div class="sub">Predict from simulator alarms · lock unsafe components · repair only in safe windows</div>
</div>
<div class="toolbar">
  <span class="mini">Simulator time: {{sim_time or 'waiting for signed simulator event'}}</span>
  <form method="post" action="/maintenance/refresh">
    <button class="gray">Refresh Simulator Maintenance Snapshot</button>
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

{% if online %}
<div class="banner good">
<b>Simulator connected.</b>
Last maintenance API snapshot: {{last_refresh or 'this session'}}.
The dashboard itself does not continuously poll the simulator; normal page refreshes use stored API data + signed webhooks.
</div>
{% else %}
<div class="banner bad">
<b>Simulator currently unavailable.</b>
Start the simulator and load Level 2, then press <b>Refresh Simulator Maintenance Snapshot</b>.
The dashboard will not invent replacement values while the simulator is offline.
</div>
{% endif %}

<div class="banner info">
<b>No fake life percentage.</b>
Your actual Level 2 API exposes no remaining-life %, cycle limit, runtime limit, or health %.
Predictive maintenance therefore comes from <b>/list-alarms</b>.
The dashboard separately shows real API state, simulator occupancy, signed break/fix events and real penalty webhooks.
</div>

<div class="kpis">
  <div class="kpi"><div class="v bad">{{stats.broken}}</div><div class="k">Broken</div></div>
  <div class="kpi"><div class="v warn">{{stats.due}}</div><div class="k">Predictive Maintenance Due</div></div>
  <div class="kpi"><div class="v cyan">{{stats.locked}}</div><div class="k">Maintenance Lockout</div></div>
  <div class="kpi"><div class="v good">{{stats.ready}}</div><div class="k">Safe To Repair Now</div></div>
  <div class="kpi"><div class="v">{{stats.blocked}}</div><div class="k">Repair Blocked</div></div>
  <div class="kpi"><div class="v warn">{{stats.penalties}}</div><div class="k">Real Maintenance Penalties</div></div>
</div>

<div class="card">
<h2>1 · Predictive / Corrective Maintenance Queue</h2>
<div class="mini" style="margin-bottom:8px">
<b>PREVENTIVE</b> appears only when the simulator's `/list-alarms` names that component.
<b>URGENT</b> appears when simulator state/webhook says the component is broken.
</div>
<div class="scroll">
<table>
<tr>
  <th>Priority</th><th>Component</th><th>Zone</th><th>Real Simulator State</th>
  <th>Predictive Signal</th><th>Occupancy / Usage</th><th>Safety Decision</th><th>Action</th>
</tr>
{% for c in queue %}
<tr class="{{'brokenrow' if c.broken else ('repairrow' if c.locked else 'duerow')}}">
<td>
  {% if c.locked %}<span class="badge cyan">LOCKED</span>
  {% elif c.broken %}<span class="badge bad">URGENT</span>
  {% else %}<span class="badge warn">PREVENTIVE</span>{% endif %}
</td>
<td><b>{{c.name}}</b><div class="mini">{{c.kind}}</div></td>
<td>{{c.zone or '-'}}</td>
<td>
  {% if c.broken %}<span class="bad"><b>BROKEN</b></span>
  {% elif c.api_under_maintenance %}<span class="cyan"><b>UNDER MAINTENANCE</b></span>
  {% else %}<span class="good"><b>{{c.state}}</b></span>{% endif %}
  <div class="mini">API/webhook backed</div>
</td>
<td>
  {% if c.alarm_problem %}
    <span class="warn"><b>REQUIRE MAINTENANCE</b></span>
    <div class="mini reason">{{c.alarm_problem}}</div>
  {% elif c.broken %}
    <span class="bad">Corrective repair required</span>
  {% else %}
    <span class="mini">No active simulator maintenance alarm</span>
  {% endif %}
</td>
<td>
  {% if c.kind == 'spot' %}
    <b>{{c.detected_cars}}</b> detected car(s)
    <div class="mini">{{c.purpose or 'Park'}}</div>
  {% elif c.kind == 'gate' %}
    <b>{{c.state}}</b>
    <div class="mini">barrier state</div>
  {% elif c.kind == 'fan' %}
    <b>{{c.state}}</b>
    <div class="mini">fan state</div>
  {% else %}
    <span class="mini">API status only</span>
  {% endif %}
</td>
<td>
  {% if c.locked %}
    <span class="bad"><b>DO NOT OPERATE</b></span>
    <div class="mini reason">{{c.safe_reason}}</div>
  {% elif c.safe %}
    <span class="good"><b>REPAIR NOW</b></span>
    <div class="mini reason">{{c.safe_reason}}</div>
  {% else %}
    <span class="warn"><b>WAIT</b></span>
    <div class="mini reason">{{c.safe_reason}}</div>
  {% endif %}
</td>
<td>
  {% if c.locked %}
    <span class="mini">Locked until real component_fixed event</span>
  {% elif c.kind not in ['spot','gate','fan'] %}
    <span class="mini">No documented repair API</span>
  {% elif c.safe %}
    <form method="post" action="/maintenance/repair/{{c.kind}}/{{c.name}}">
      <button class="{{'danger' if c.broken else 'good'}}">
        {{'Repair Broken' if c.broken else 'Preventive Repair'}}
      </button>
    </form>
  {% else %}
    <span class="mini">Repair disabled by interlock</span>
  {% endif %}
</td>
</tr>
{% else %}
<tr><td colspan="8" class="empty">No broken components and `/list-alarms` currently reports no predictive-maintenance items.</td></tr>
{% endfor %}
</table>
</div>
</div>

<div class="grid">

<div class="card">
<h2>2 · Maintenance Lockout</h2>
<div class="mini" style="margin-bottom:8px">
A component is locked when the API says <b>isUnderMaintenance=true</b> or PARKMIND has a real accepted repair command waiting for a signed fixed event.
This maintenance screen exposes no operational controls for locked components.
</div>
<table>
<tr><th>Component</th><th>Kind</th><th>Zone</th><th>Reason</th></tr>
{% for c in locked %}
<tr class="brokenrow">
  <td><b>{{c.name}}</b></td><td>{{c.kind}}</td><td>{{c.zone or '-'}}</td>
  <td class="bad"><b>{{c.lock_reason}}</b></td>
</tr>
{% else %}
<tr><td colspan="4" class="empty">No components are currently locked for maintenance.</td></tr>
{% endfor %}
</table>
</div>

<div class="card">
<h2>3 · Repair Safety Interlocks</h2>
<div class="statline"><span>Occupied parking spots blocked</span><b>{{interlocks.occupied}}</b></div>
<div class="statline"><span>Entry/Exit sensors not offered for repair</span><b>{{interlocks.sensors}}</b></div>
<div class="statline"><span>Gates waiting to be closed</span><b>{{interlocks.gates}}</b></div>
<div class="statline"><span>Healthy fans retained for CO safety</span><b>{{interlocks.fans}}</b></div>
<div class="mini" style="margin-top:9px">
The Repair endpoint re-checks the <b>live simulator API immediately before sending a repair command</b>.
So a stale browser page cannot repair a parking spot that has since become occupied.
</div>
</div>

</div>

<div class="two">

<div class="card">
<h2>4 · Real Repair Status</h2>
<table>
<tr><th>Component</th><th>Type</th><th>Started</th><th>Completed</th><th>Status</th></tr>
{% for j in jobs %}
<tr>
  <td><b>{{j.component}}</b><div class="mini">{{j.kind}}</div></td>
  <td>{{j.repair_type}}</td>
  <td>{{j.start_sim_time or j.started_at}}</td>
  <td>{{j.completed_sim_time or '-'}}</td>
  <td><span class="badge {{'good' if j.status=='COMPLETED' else ('bad' if j.status=='FAILED' else 'warn')}}">{{j.status}}</span></td>
</tr>
{% else %}
<tr><td colspan="5" class="empty">No maintenance repair commands recorded by this dashboard.</td></tr>
{% endfor %}
</table>
<div class="mini" style="margin-top:8px">
A repair is marked COMPLETED only when PARKMIND has stored a real simulator <b>component_fixed</b> webhook for that component.
</div>
</div>

<div class="card">
<h2>5 · Real Simulator Maintenance Penalties</h2>
<div class="scroll">
<table>
<tr><th>Simulator Time</th><th>Component</th><th>Type</th><th>Reason</th><th>Fine</th></tr>
{% for p in penalties %}
<tr>
  <td>{{p.simulator_time or '-'}}</td>
  <td><b>{{p.component or '-'}}</b></td>
  <td>{{p.component_type or '-'}}</td>
  <td class="reason">{{p.reason}}</td>
  <td class="bad"><b>{{p.fine}}</b></td>
</tr>
{% else %}
<tr><td colspan="5" class="empty">No maintenance-related penalty webhook has been received.</td></tr>
{% endfor %}
</table>
</div>
<div class="mini" style="margin-top:8px">No penalty is created or estimated by this dashboard.</div>
</div>

</div>

<div class="two">

<div class="card">
<h2>6 · CO / Exhaust Fan Safety</h2>
{% for z in zones %}
<div class="zone {{'danger' if z.is_danger else ''}}">
  <div style="display:flex;justify-content:space-between">
    <b>{{z.name}}</b>
    <span class="{{'bad' if z.is_danger else 'good'}}">{{z.risk}}</span>
  </div>
  <div class="mini">CO={{z.co_level}} · from simulator `/list-zones`</div>
  {% for f in z.fans %}
    <div style="margin-top:5px">
      <b>{{f.name}}</b> · {{f.state}}
      {% if f.broken %}<span class="bad">BROKEN</span>{% endif %}
      {% if f.locked %}<span class="bad">LOCKED</span>{% endif %}
    </div>
  {% else %}
    <div class="mini">No fan returned for this zone.</div>
  {% endfor %}
</div>
{% else %}
<div class="empty">No zone snapshot. Press Refresh Simulator Maintenance Snapshot.</div>
{% endfor %}
</div>

<div class="card">
<h2>7 · Lights — Actual API Limits</h2>
<div class="mini" style="margin-bottom:8px">
Your actual Level 2 `/list-lights` returns only <b>name, group, zoneParent, isOn</b>.
It does not expose broken, isUnderMaintenance, remaining life, or a light repair endpoint.
Therefore this maintenance view does not invent those fields.
</div>
<table>
<tr><th>Light</th><th>Zone</th><th>Group</th><th>State</th></tr>
{% for l in lights %}
<tr><td><b>{{l.name}}</b></td><td>{{l.zone or '-'}}</td><td>{{l.group_name or '-'}}</td><td>{{l.state}}</td></tr>
{% else %}
<tr><td colspan="4" class="empty">No light snapshot loaded.</td></tr>
{% endfor %}
</table>
</div>

</div>

<details>
<summary>All maintenance API components</summary>
<div class="scroll">
<table>
<tr><th>Name</th><th>Kind</th><th>Zone</th><th>State</th><th>Broken</th><th>Under Maintenance</th><th>Alarm</th></tr>
{% for c in components %}
<tr>
  <td><b>{{c.name}}</b></td><td>{{c.kind}}</td><td>{{c.zone or '-'}}</td><td>{{c.state}}</td>
  <td>{{'YES' if c.broken else ('N/A' if c.broken is none else 'No')}}</td>
  <td>{{'YES' if c.api_under_maintenance else ('N/A' if c.api_under_maintenance is none else 'No')}}</td>
  <td>{{c.alarm_problem or 'None'}}</td>
</tr>
{% endfor %}
</table>
</div>
</details>

</div>

<script>
// UI refresh only. This does NOT call simulator list APIs.
setTimeout(() => window.location.reload(), 5000);
</script>
</body>
</html>
"""


# ============================================================
# DATABASE HELPERS — private tables owned by this dashboard
# ============================================================
def db():
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def table_exists(conn, name):
    return bool(conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,)
    ).fetchone())


def column_names(conn, table):
    if not table_exists(conn, table):
        return set()
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


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
        raw_json TEXT,
        synced_at TEXT,
        PRIMARY KEY(name, kind)
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
    """)
    conn.commit()
    conn.close()


def set_meta(key, value):
    conn = db()
    conn.execute(
        """INSERT INTO maintenance_meta(key,value) VALUES(?,?)
           ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
        (key, str(value))
    )
    conn.commit()
    conn.close()


def get_meta(key, default=""):
    conn = db()
    row = conn.execute(
        "SELECT value FROM maintenance_meta WHERE key=?",
        (key,)
    ).fetchone()
    conn.close()
    return row["value"] if row else default


def get_sim_time(conn=None):
    own = False
    if conn is None:
        conn = db()
        own = True
    value = ""
    if table_exists(conn, "system_state"):
        row = conn.execute(
            "SELECT value FROM system_state WHERE key='last_simulator_time'"
        ).fetchone()
        if row:
            value = row["value"] or ""
    if own:
        conn.close()
    return value


# ============================================================
# SIMULATOR API — maintenance-only discovery and repairs
# ============================================================
def sim_login():
    global TOKEN
    r = requests.post(
        f"{SIM_BASE}/auth/login",
        json={"email": SIM_USER, "password": SIM_PASSWORD},
        timeout=7
    )
    r.raise_for_status()
    body = r.json()
    TOKEN = body.get("token") or body.get("accessToken") or body.get("access_token")
    if not TOKEN:
        raise RuntimeError("Simulator authentication returned no token")
    return TOKEN


def sim_request(method, path, **kwargs):
    global TOKEN
    if not TOKEN:
        sim_login()

    headers = kwargs.pop("headers", {})
    headers["Authorization"] = f"Bearer {TOKEN}"

    r = requests.request(
        method,
        f"{SIM_BASE}{path}",
        headers=headers,
        timeout=8,
        **kwargs
    )

    if r.status_code == 401:
        sim_login()
        headers["Authorization"] = f"Bearer {TOKEN}"
        r = requests.request(
            method,
            f"{SIM_BASE}{path}",
            headers=headers,
            timeout=8,
            **kwargs
        )

    r.raise_for_status()
    return r


def detected_count(value):
    if isinstance(value, list):
        return len(value)
    try:
        return int(value or 0)
    except Exception:
        return 0


def refresh_snapshot():
    """
    One explicit simulator maintenance snapshot.
    Not used by the 5-second UI refresh.
    """
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    endpoints = {
        "spot": "/list-parking-spots",
        "gate": "/list-barriers",
        "light": "/list-lights",
        "fan": "/list-exhaust-fans",
    }

    pulled = {}
    for kind, endpoint in endpoints.items():
        data = sim_request("GET", endpoint).json()
        pulled[kind] = data if isinstance(data, list) else []

    alarms = sim_request("GET", "/list-alarms").json()
    if not isinstance(alarms, list):
        alarms = []

    zones = sim_request("GET", "/list-zones").json()
    if not isinstance(zones, list):
        zones = []

    conn = db()
    conn.execute("DELETE FROM maintenance_api_snapshot")
    conn.execute("DELETE FROM maintenance_alarm_snapshot")
    conn.execute("DELETE FROM maintenance_zone_snapshot")

    for kind, items in pulled.items():
        for item in items:
            name = str(item.get("name") or "").strip()
            if not name:
                continue

            zone = str(item.get("zoneParent") or "").strip()
            purpose = str(item.get("purpose") or "").strip()

            broken = None if kind == "light" else int(bool(item.get("broken", False)))
            under = None if kind == "light" else int(bool(item.get("isUnderMaintenance", False)))

            if kind == "spot":
                if purpose == "Park":
                    count = detected_count(item.get("detectedCars"))
                    state = "Occupied" if count > 0 else "Free"
                else:
                    count = detected_count(item.get("detectedCars"))
                    state = purpose or "Sensor"
            elif kind == "gate":
                count = 0
                state = str(item.get("state") or "Unknown")
            elif kind in ("fan", "light"):
                count = 0
                state = "On" if bool(item.get("isOn")) else "Off"
            else:
                count = 0
                state = "Unknown"

            conn.execute(
                """INSERT INTO maintenance_api_snapshot(
                   name,kind,zone,state,broken,api_under_maintenance,
                   detected_cars,purpose,group_name,raw_json,synced_at
                   ) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    name, kind, zone, state, broken, under,
                    count, purpose,
                    str(item.get("group") or ""),
                    json.dumps(item),
                    now
                )
            )

    for alarm in alarms:
        name = str(alarm.get("name") or alarm.get("Name") or "").strip()
        if not name:
            continue
        problem = str(alarm.get("problem") or alarm.get("Problem") or "Require Maintenance")
        conn.execute(
            """INSERT INTO maintenance_alarm_snapshot(name,problem,raw_json,synced_at)
               VALUES(?,?,?,?)""",
            (name, problem, json.dumps(alarm), now)
        )

    for zone in zones:
        name = str(zone.get("name") or "").strip()
        if not name:
            continue
        conn.execute(
            """INSERT INTO maintenance_zone_snapshot(name,co_level,risk,raw_json,synced_at)
               VALUES(?,?,?,?,?)""",
            (
                name,
                zone.get("gasCarbonMonoxideLevel"),
                str(zone.get("risk") or "Unknown"),
                json.dumps(zone),
                now
            )
        )

    conn.commit()
    conn.close()

    set_meta("last_refresh", now)
    set_meta("last_refresh_ok", "1")
    return True


def live_component(kind, name):
    endpoint = {
        "spot": "/list-parking-spots",
        "gate": "/list-barriers",
        "fan": "/list-exhaust-fans",
    }.get(kind)

    if not endpoint:
        return None

    items = sim_request("GET", endpoint).json()
    if not isinstance(items, list):
        return None

    for item in items:
        if str(item.get("name") or "") == name:
            return item
    return None


def live_zone(zone_name):
    zones = sim_request("GET", "/list-zones").json()
    if not isinstance(zones, list):
        return None
    for zone in zones:
        if str(zone.get("name") or "") == zone_name:
            return zone
    return None


def risk_requires_fan(risk):
    return str(risk or "").strip().lower() in {
        "mid", "moderate", "high", "critical", "danger", "unsafe"
    }


# ============================================================
# SIGNED EVENT RECONCILIATION
# ============================================================
def reconcile_signed_events():
    """
    Uses already-stored PARKMIND events; no simulator polling.

    component_broken -> mark snapshot broken
    component_fixed  -> unlock + complete pending local repair job + clear alarm
    """
    conn = db()

    if not table_exists(conn, "events"):
        conn.close()
        return

    cols = column_names(conn, "events")
    required = {"event_class", "payload"}
    if not required.issubset(cols):
        conn.close()
        return

    rows = conn.execute(
        """SELECT event_class,server_time,payload
           FROM events
           WHERE event_class IN('component_broken','component_fixed')
           ORDER BY id ASC"""
    ).fetchall()

    latest = {}
    for row in rows:
        try:
            payload = json.loads(row["payload"] or "{}")
        except Exception:
            continue
        name = str(payload.get("Name") or "").strip()
        if not name:
            continue
        latest[name] = {
            "event_class": row["event_class"],
            "server_time": row["server_time"] or payload.get("ServerDateTime") or "",
        }

    for name, event in latest.items():
        if event["event_class"] == "component_broken":
            conn.execute(
                """UPDATE maintenance_api_snapshot
                   SET broken=1
                   WHERE name=? AND broken IS NOT NULL""",
                (name,)
            )
        elif event["event_class"] == "component_fixed":
            conn.execute(
                """UPDATE maintenance_api_snapshot
                   SET broken=0,api_under_maintenance=0
                   WHERE name=? AND broken IS NOT NULL""",
                (name,)
            )
            conn.execute(
                "DELETE FROM maintenance_alarm_snapshot WHERE name=?",
                (name,)
            )
            conn.execute(
                """UPDATE maintenance_dashboard_jobs
                   SET status='COMPLETED',completed_sim_time=?
                   WHERE component=? AND status='COMMAND_SENT'""",
                (event["server_time"], name)
            )

    conn.commit()
    conn.close()


# ============================================================
# DASHBOARD DECISIONS
# ============================================================
def get_pending_job_names(conn):
    return {
        row["component"]
        for row in conn.execute(
            """SELECT component FROM maintenance_dashboard_jobs
               WHERE status='COMMAND_SENT'"""
        ).fetchall()
    }


def safe_decision(component, zones_by_name):
    if component["locked"]:
        return False, component["lock_reason"]

    kind = component["kind"]

    if kind == "spot":
        if component["purpose"] != "Park":
            return False, "Entry/Exit sensors are not offered as normal parking-spot repairs"

        if int(component["detected_cars"] or 0) > 0:
            return False, f"Simulator currently detects {component['detected_cars']} car(s) in this spot"

        return True, "Simulator snapshot reports this parking spot free"

    if kind == "gate":
        if component["broken"]:
            return True, "Gate is broken and unavailable; corrective repair can start"

        if str(component["state"]).lower() != "closed":
            return False, f"Gate is {component['state']}; preventive repair waits until Closed"

        return True, "Simulator reports gate Closed"

    if kind == "fan":
        if component["broken"]:
            return True, "Fan is already broken/unavailable; corrective repair can start"

        zone = zones_by_name.get(component["zone"])
        if zone and risk_requires_fan(zone["risk"]):
            return False, f"{component['zone']} risk is {zone['risk']}; keep healthy ventilation available"

        if str(component["state"]).lower() != "off":
            return False, "Fan must be Off before preventive repair"

        return True, "Fan is Off and zone has no active ventilation warning"

    return False, "No documented repair API for this component type"


def maintenance_penalties(conn):
    if not table_exists(conn, "penalties"):
        return []

    cols = column_names(conn, "penalties")
    if not {"reason", "fine_amount"}.issubset(cols):
        return []

    rows = [
        dict(row)
        for row in conn.execute(
            "SELECT * FROM penalties ORDER BY id DESC"
        ).fetchall()
    ]

    relevant = []
    for row in rows:
        reason = str(row.get("reason") or "")
        low = reason.lower()

        try:
            payload = json.loads(row.get("payload") or "{}")
        except Exception:
            payload = {}

        if not any(word in low for word in (
            "maintenance", "repair", "broken", "under maintenance", "occupied"
        )):
            # Also retain explicitly component-based simulator penalties.
            if str(payload.get("Type") or "").lower() not in {
                "barriergate", "parkingspot", "exhaustfan", "light"
            }:
                continue

        component = payload.get("ComponentName") or payload.get("Name") or ""
        component_type = payload.get("Type") or ""

        try:
            fine = f"{float(row.get('fine_amount') or 0):.2f}"
        except Exception:
            fine = str(row.get("fine_amount") or "")

        relevant.append({
            "simulator_time": row.get("simulator_time") or "",
            "component": component,
            "component_type": component_type,
            "reason": reason,
            "fine": fine,
        })

    return relevant


# ============================================================
# ROUTES
# ============================================================
@maintenance_bp.route("/refresh", methods=["POST"])
def refresh():
    if not session.get("user"):
        return redirect("/login")
    if session.get("role") != "Maintenance":
        return redirect("/")

    try:
        refresh_snapshot()
        flash("Maintenance snapshot refreshed from the real Level 2 simulator API.")
    except Exception as exc:
        set_meta("last_refresh_ok", "0")
        flash(f"Simulator refresh failed: {exc}")

    return redirect("/maintenance/")


@maintenance_bp.route("/repair/<kind>/<name>", methods=["POST"])
def repair(kind, name):
    if not session.get("user"):
        return redirect("/login")
    if session.get("role") != "Maintenance":
        return redirect("/")

    if kind not in ("spot", "gate", "fan"):
        flash("Repair blocked: no documented repair endpoint for this component type.")
        return redirect("/maintenance/")

    # Re-check LIVE simulator state immediately before repair.
    try:
        item = live_component(kind, name)
        if not item:
            flash(f"Repair blocked: {name} was not found in the live simulator API.")
            return redirect("/maintenance/")

        if bool(item.get("isUnderMaintenance", False)):
            flash(f"Repair blocked: {name} is already under maintenance.")
            return redirect("/maintenance/")

        broken = bool(item.get("broken", False))
        zone = str(item.get("zoneParent") or "")

        if kind == "spot":
            purpose = str(item.get("purpose") or "")
            cars = detected_count(item.get("detectedCars"))

            if purpose != "Park":
                flash(f"Repair blocked: {name} is {purpose}, not a normal parking bay.")
                return redirect("/maintenance/")

            if cars > 0:
                flash(
                    f"Repair blocked: simulator currently detects {cars} car(s) in {name}. "
                    "Occupied parking spots cannot be repaired."
                )
                return redirect("/maintenance/")

        elif kind == "gate":
            state = str(item.get("state") or "")
            if not broken and state.lower() != "closed":
                flash(f"Repair blocked: {name} is {state}. Close/idle the gate first.")
                return redirect("/maintenance/")

        elif kind == "fan":
            is_on = bool(item.get("isOn", False))
            if not broken:
                zone_data = live_zone(zone)
                if zone_data and risk_requires_fan(zone_data.get("risk")):
                    flash(
                        f"Repair blocked: {zone} CO risk is {zone_data.get('risk')}. "
                        "Keep healthy ventilation available."
                    )
                    return redirect("/maintenance/")

                if is_on:
                    flash(f"Repair blocked: {name} is On. Turn it Off before preventive repair.")
                    return redirect("/maintenance/")

        endpoint = {
            "spot": f"/parking-spots/{quote(name, safe='')}/repair",
            "gate": f"/barrier-gates/{quote(name, safe='')}/repair",
            "fan": f"/exhaust-fans/{quote(name, safe='')}/repair",
        }[kind]

        # Decide preventive vs corrective from real state/alarm snapshot.
        conn = db()
        alarm = conn.execute(
            "SELECT problem FROM maintenance_alarm_snapshot WHERE name=?",
            (name,)
        ).fetchone()
        sim_time = get_sim_time(conn)
        repair_type = "CORRECTIVE" if broken else "PREVENTIVE" if alarm else "MANUAL"

        existing = conn.execute(
            """SELECT 1 FROM maintenance_dashboard_jobs
               WHERE component=? AND status='COMMAND_SENT'""",
            (name,)
        ).fetchone()
        if existing:
            conn.close()
            flash(f"Repair blocked: a repair command for {name} is already pending.")
            return redirect("/maintenance/")
        conn.close()

        sim_request("POST", endpoint)

        conn = db()
        conn.execute(
            """INSERT INTO maintenance_dashboard_jobs(
               component,kind,repair_type,started_at,start_sim_time,status,detail
               ) VALUES(?,?,?,?,?,?,?)""",
            (
                name, kind, repair_type,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                sim_time or None,
                "COMMAND_SENT",
                "Simulator repair endpoint accepted the command"
            )
        )
        conn.commit()
        conn.close()

        flash(
            f"{repair_type.title()} repair command accepted for {name}. "
            "PARKMIND now locks it until a real component_fixed event is stored."
        )

    except Exception as exc:
        flash(f"Repair command failed: {exc}")

    return redirect("/maintenance/")


@maintenance_bp.route("/")
def dashboard():
    if not session.get("user"):
        return redirect("/login")
    if session.get("role") != "Maintenance":
        return redirect("/")

    init_maintenance_tables()

    # If no snapshot exists yet, attempt ONE initial maintenance discovery.
    conn = db()
    snapshot_count = conn.execute(
        "SELECT COUNT(*) FROM maintenance_api_snapshot"
    ).fetchone()[0]
    conn.close()

    if snapshot_count == 0:
        try:
            refresh_snapshot()
        except Exception:
            set_meta("last_refresh_ok", "0")

    # Update snapshot from signed component break/fix events without polling.
    reconcile_signed_events()

    conn = db()
    sim_time = get_sim_time(conn)

    component_rows = [
        dict(row)
        for row in conn.execute(
            """SELECT s.*,a.problem AS alarm_problem
               FROM maintenance_api_snapshot s
               LEFT JOIN maintenance_alarm_snapshot a ON a.name=s.name
               ORDER BY s.kind,s.zone,s.name"""
        ).fetchall()
    ]

    zones = [
        dict(row)
        for row in conn.execute(
            "SELECT * FROM maintenance_zone_snapshot ORDER BY name"
        ).fetchall()
    ]
    zones_by_name = {z["name"]: z for z in zones}

    pending_names = get_pending_job_names(conn)

    components = []
    locked = []
    queue = []
    interlocks = {"occupied": 0, "sensors": 0, "gates": 0, "fans": 0}

    for c in component_rows:
        c["broken"] = None if c["broken"] is None else bool(c["broken"])
        c["api_under_maintenance"] = (
            None if c["api_under_maintenance"] is None
            else bool(c["api_under_maintenance"])
        )

        pending = c["name"] in pending_names
        c["locked"] = bool(c["api_under_maintenance"]) or pending

        if c["api_under_maintenance"]:
            c["lock_reason"] = "Simulator API: isUnderMaintenance=true"
        elif pending:
            c["lock_reason"] = "Repair command accepted; awaiting signed component_fixed event"
        else:
            c["lock_reason"] = ""

        safe, safe_reason = safe_decision(c, zones_by_name)
        c["safe"] = safe
        c["safe_reason"] = safe_reason

        if c["locked"]:
            locked.append(c)

        if c["broken"] or c["alarm_problem"] or c["locked"]:
            queue.append(c)

        if c["kind"] == "spot":
            if c["purpose"] != "Park":
                interlocks["sensors"] += 1
            elif int(c["detected_cars"] or 0) > 0:
                interlocks["occupied"] += 1
        elif c["kind"] == "gate" and not c["broken"] and not c["locked"]:
            if str(c["state"]).lower() != "closed":
                interlocks["gates"] += 1
        elif c["kind"] == "fan" and not c["broken"] and not c["locked"]:
            zone = zones_by_name.get(c["zone"])
            if zone and risk_requires_fan(zone["risk"]):
                interlocks["fans"] += 1

        components.append(c)

    # Priority: broken first, predictive second, already-in-repair third.
    queue.sort(
        key=lambda c: (
            0 if c["broken"] else 1 if c["alarm_problem"] and not c["locked"] else 2,
            c["zone"] or "",
            c["name"]
        )
    )

    # Attach fans to zones.
    fans = [c for c in components if c["kind"] == "fan"]
    zone_cards = []
    for z in zones:
        z = dict(z)
        z["co_level"] = z.get("co_level")
        z["risk"] = z.get("risk") or "Unknown"
        z["is_danger"] = risk_requires_fan(z["risk"])
        z["fans"] = [f for f in fans if f["zone"] == z["name"]]
        zone_cards.append(z)

    lights = [c for c in components if c["kind"] == "light"]

    jobs = [
        dict(row)
        for row in conn.execute(
            """SELECT * FROM maintenance_dashboard_jobs
               ORDER BY id DESC LIMIT 30"""
        ).fetchall()
    ]

    penalties = maintenance_penalties(conn)

    stats = {
        "broken": sum(1 for c in components if c["broken"] is True),
        "due": sum(
            1 for c in components
            if c["alarm_problem"] and not c["broken"] and not c["locked"]
        ),
        "locked": len(locked),
        "ready": sum(
            1 for c in queue
            if c["safe"] and not c["locked"] and c["kind"] in ("spot", "gate", "fan")
        ),
        "blocked": sum(
            1 for c in queue
            if not c["safe"] and not c["locked"]
        ),
        "penalties": len(penalties),
    }

    conn.close()

    return render_template_string(
        HTML,
        sim_time=sim_time,
        online=(get_meta("last_refresh_ok", "0") == "1"),
        last_refresh=get_meta("last_refresh", ""),
        stats=stats,
        queue=queue,
        locked=locked,
        interlocks=interlocks,
        jobs=jobs,
        penalties=penalties[:20],
        zones=zone_cards,
        lights=lights,
        components=components,
    )
