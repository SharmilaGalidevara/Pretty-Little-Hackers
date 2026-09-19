from flask import Blueprint, render_template_string, session, redirect
import sqlite3
from pathlib import Path
import os
import json
import re
from datetime import datetime

# ============================================================
# PARKMIND LEVEL 2 — OPERATOR DASHBOARD
# ============================================================
# Operational view only:
# - 90 real parking bays from simulator API
# - 3 zones
# - live cars / routes
# - gates
# - CO + fans
# - critical operational alerts
# - paid-only exit state
#
# NO revenue / financial reports / maintenance planning.
# ============================================================

operator_bp = Blueprint("operator", __name__, url_prefix="/operator")

APP_DIR = Path(__file__).resolve().parent
DB_PATH = Path(
    os.getenv("PARKMIND_LEVEL2_DB", str(APP_DIR / "parkmind_level2.db"))
)

# Real Level 2 sensor names discovered from the simulator topology.
ZONE_FLOW = {
    "ZONE1": {"entry": "ENTRY1", "exit": "EXIT_EXIT"},
    "ZONE2": {"entry": "ENTRY2", "exit": "Exit67"},
    "ZONE3": {"entry": "ENTRY3", "exit": "Exit100"},
}

HTML = """
<!doctype html>
<html>
<head>
<title>PARKMIND — Level 2 Operator</title>
<style>
*{box-sizing:border-box}
body{
    margin:0;background:#07111f;color:#e5e7eb;
    font-family:'Segoe UI',Arial,sans-serif
}
header{
    min-height:66px;padding:10px 18px;background:#0e1a2c;
    border-bottom:1px solid #26364e;
    display:flex;align-items:center;justify-content:space-between;gap:14px
}
.brand{font-size:22px;font-weight:900;color:#67e8f9}
.sub,.mini{font-size:10px;color:#94a3b8}
a{color:#67e8f9;text-decoration:none}
.page{max-width:1550px;margin:auto;padding:12px}
.role{
    display:inline-block;background:#1e293b;color:#cbd5e1;
    padding:5px 9px;border-radius:999px;font-size:10px;font-weight:800
}
.banner{
    padding:9px 11px;border-radius:9px;margin-bottom:10px;
    background:#082f49;border:1px solid #0e7490;
    color:#bae6fd;font-size:10px;line-height:1.45
}
.metrics{
    display:grid;
    grid-template-columns:repeat(auto-fit,minmax(130px,1fr));
    gap:8px;margin-bottom:10px
}
.metric,.card{
    background:#101b2d;border:1px solid #26364e;
    border-radius:12px;padding:11px;min-width:0
}
.metric .v{font-size:22px;font-weight:900}
.metric .k{
    color:#94a3b8;font-size:9px;text-transform:uppercase;
    letter-spacing:.5px
}
.good{color:#86efac}.warn{color:#fbbf24}.bad{color:#f87171}.cyan{color:#67e8f9}
.grid{display:grid;grid-template-columns:1.2fr .8fr;gap:10px}
.two{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:10px}
h2{
    margin:0 0 8px;font-size:13px;text-transform:uppercase;
    letter-spacing:.65px;color:#cbd5e1
}
table{width:100%;border-collapse:collapse;font-size:10px}
th,td{
    padding:7px;border-bottom:1px solid #26364e;
    text-align:left;vertical-align:top;white-space:nowrap
}
th{font-size:9px;color:#94a3b8;text-transform:uppercase}
.scroll{max-height:355px;overflow:auto}
.compact-scroll{max-height:245px;overflow:auto}
.status{
    display:inline-block;padding:3px 6px;border-radius:999px;
    background:#1e293b;font-size:9px;font-weight:800
}
.status.good{background:#0f2f25}.status.bad{background:#3a171b}
.status.warn{background:#3b2a10}.status.cyan{background:#082f49}
button{
    padding:5px 8px;border:0;border-radius:7px;
    background:#0284c7;color:#fff;font-size:9px;
    font-weight:700;cursor:pointer;margin:1px
}
button.gray{background:#475569}button.danger{background:#b91c1c}
form{display:inline}
.alert{
    display:grid;grid-template-columns:105px 95px 85px 1fr;
    gap:7px;align-items:center;padding:8px;margin:6px 0;
    background:#2a1217;border-left:4px solid #ef4444;
    border-radius:8px;font-size:10px
}
.alert.high{background:#251a10;border-left-color:#f59e0b}
.alert .reason{white-space:normal;line-height:1.3}
.empty{padding:16px;text-align:center;color:#64748b;font-size:10px}

.zone-grid{
    display:grid;grid-template-columns:repeat(3,minmax(0,1fr));
    gap:8px
}
.zone-card{
    background:#0a1424;border:1px solid #26364e;
    border-radius:10px;padding:10px
}
.zone-card.danger{background:#281116;border-color:#7f1d1d}
.zone-title{
    display:flex;justify-content:space-between;gap:8px;
    align-items:flex-start
}
.zone-name{font-size:15px;font-weight:900}
.zone-flow{
    margin-top:7px;padding:6px;border-radius:7px;
    background:#0d1727;font-size:9px;color:#cbd5e1
}
.bar{
    height:6px;background:#1e293b;border-radius:6px;
    overflow:hidden;margin-top:6px
}
.bar span{display:block;height:100%;background:#38bdf8}
.zone-stats{
    display:grid;grid-template-columns:repeat(4,1fr);
    gap:5px;margin-top:7px
}
.zone-stat{
    text-align:center;padding:5px 3px;background:#0d1727;
    border-radius:6px
}
.zone-stat b{display:block;font-size:13px}
.zone-stat small{font-size:8px;color:#94a3b8}

.gate{
    display:flex;justify-content:space-between;align-items:center;
    gap:8px;padding:7px 0;border-bottom:1px solid #26364e
}
.lock{color:#f87171;font-size:9px;font-weight:800}
.route{font-size:9px;color:#94a3b8;white-space:normal;line-height:1.3}

.map-zone{
    margin-bottom:10px;border:1px solid #26364e;
    border-radius:9px;padding:8px;background:#0a1424
}
.map-head{
    display:flex;justify-content:space-between;gap:8px;
    align-items:center;margin-bottom:7px
}
.spots{
    display:grid;grid-template-columns:repeat(10,minmax(0,1fr));
    gap:4px
}
.spot{
    min-height:34px;padding:4px 2px;border-radius:5px;text-align:center;
    font-size:8px;background:#0f2f25;color:#86efac;
    border:1px solid #14532d;overflow:hidden
}
.spot.busy{background:#3a171b;color:#fca5a5;border-color:#7f1d1d}
.spot.res{background:#3b2a10;color:#fde68a;border-color:#92400e}
.spot.broken{background:#3b1111;color:#fca5a5;border-color:#ef4444}
.spot.maint{background:#172554;color:#93c5fd;border-color:#2563eb}
.spot .type{display:block;font-size:7px;opacity:.8;margin-top:2px}
.legend{display:flex;gap:8px;flex-wrap:wrap;font-size:9px;color:#94a3b8;margin-top:6px}
.dot{display:inline-block;width:8px;height:8px;border-radius:2px;margin-right:3px;vertical-align:middle}
.dot.free{background:#0f2f25}.dot.busy{background:#3a171b}.dot.res{background:#3b2a10}.dot.bad{background:#3b1111}.dot.maint{background:#172554}

.loginrow{padding:6px 0;border-bottom:1px solid #26364e;font-size:10px}
.toolbar{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
details{
    margin-top:10px;background:#0b1424;border:1px solid #26364e;
    border-radius:10px;padding:8px
}
summary{cursor:pointer;font-weight:800;font-size:10px;color:#cbd5e1}

@media(max-width:1050px){
    .grid,.two{grid-template-columns:1fr}
    .zone-grid{grid-template-columns:1fr}
}
@media(max-width:700px){
    .spots{grid-template-columns:repeat(5,1fr)}
    .alert{grid-template-columns:95px 80px 1fr}
    .alert .time{display:none}
}
</style>
</head>

<body>
<header>
<div>
    <div class="brand">PARKMIND · Operator</div>
    <div class="sub">Level 2 · live vehicle flow, incidents, gates and zone safety</div>
</div>
<div class="toolbar">
    <span class="mini">Simulator: {{sim_time or 'waiting for event'}}</span>
    <span class="role">Operator · {{user}}</span>
    <a href="/logout">Logout</a>
</div>
</header>

<div class="page">

<div class="banner">
<b>LEVEL 2 OPERATIONS:</b>
{{capacity.total}} real parking bays across 3 zones.
The operator sees live movement, zone load, gates, CO safety and payment holds.
Financial reports and maintenance planning stay out of this screen.
<b>No valid payment → no exit-gate release.</b>
</div>

<div class="metrics">
    <div class="metric">
        <div class="v cyan">{{capacity.total}}</div>
        <div class="k">Real Parking Bays</div>
    </div>
    <div class="metric">
        <div class="v">{{ops.active}}</div>
        <div class="k">Active Vehicles</div>
    </div>
    <div class="metric">
        <div class="v warn">{{ops.moving}}</div>
        <div class="k">Moving / In Transit</div>
    </div>
    <div class="metric">
        <div class="v good">{{ops.parked}}</div>
        <div class="k">Parked</div>
    </div>
    <div class="metric">
        <div class="v warn">{{ops.at_exit}}</div>
        <div class="k">At / Going To Exit</div>
    </div>
    <div class="metric">
        <div class="v bad">{{ops.alerts}}</div>
        <div class="k">Critical Alerts</div>
    </div>
    <div class="metric">
        <div class="v {{'bad' if ops.unsafe_zones else 'good'}}">{{ops.unsafe_zones}}</div>
        <div class="k">CO Risk Zones</div>
    </div>
</div>

<div class="zone-grid">
{% for z in zones %}
<div class="zone-card {{'danger' if z.is_danger else ''}}">
    <div class="zone-title">
        <div>
            <div class="zone-name">{{z.name}}</div>
            <div class="mini">{{z.total}} real bays · {{z.active_cars}} active car(s)</div>
        </div>
        <div class="{{'bad' if z.is_danger else 'good'}}">
            <b>{{z.risk or 'Unknown'}}</b>
            <div class="mini">CO {{z.co_level if z.co_level is not none else '-'}}</div>
        </div>
    </div>

    <div class="zone-stats">
        <div class="zone-stat"><b>{{z.occupied}}</b><small>OCCUPIED</small></div>
        <div class="zone-stat"><b>{{z.reserved}}</b><small>RESERVED</small></div>
        <div class="zone-stat"><b>{{z.free}}</b><small>AVAILABLE</small></div>
        <div class="zone-stat"><b>{{z.moving}}</b><small>MOVING</small></div>
    </div>

    <div class="bar"><span style="width:{{z.percent}}%"></span></div>

    <div class="zone-flow">
        <b>Flow:</b> {{z.entry}} → {{z.name}} parking → {{z.exit}}
        <br><b>Fans:</b> {{z.fans}}
    </div>
</div>
{% endfor %}
</div>

<div class="grid" style="margin-top:10px">

<div class="card">
<h2>🚨 Critical Operational Alerts</h2>
<div class="compact-scroll">
{% for a in alerts %}
<div class="alert {{'high' if a.severity=='HIGH' else ''}}">
    <div>
        <b class="{{'bad' if a.severity=='CRITICAL' else 'warn'}}">{{a.alert_type}}</b>
        <div class="mini">{{a.severity}}</div>
    </div>
    <div>
        <b>{{a.plate or a.component or '-'}}</b>
        <div class="mini">{{a.zone or '-'}}</div>
    </div>
    <div class="time">
        {{a.simulator_time[11:19] if a.simulator_time else '-'}}
    </div>
    <div class="reason">{{a.reason}}</div>
</div>
{% else %}
<div class="empty">No active operational alerts.</div>
{% endfor %}
</div>
</div>

<div class="card">
<h2>Gate Control · Level 2</h2>
<div class="compact-scroll">
{% for g in gates %}
<div class="gate">
    <div>
        <b>{{g.name}}</b>
        <div class="mini">
            {{g.zone or 'PERIMETER'}} · {{g.state}} · {{g.role|upper}}
        </div>
        {% if g.broken %}
            <div class="lock">BROKEN · LOCKED</div>
        {% elif g.under_maintenance %}
            <div class="lock">UNDER MAINTENANCE · DO NOT OPERATE</div>
        {% endif %}
    </div>

    <div>
    {% if not g.broken and not g.under_maintenance %}
        {% if g.role == 'exit' %}
            <span class="mini">PAID-ONLY AUTO OPEN</span>
        {% else %}
            <form method="post" action="/control/gate/{{g.name}}/open">
                <button>Open</button>
            </form>
        {% endif %}
        <form method="post" action="/control/gate/{{g.name}}/close">
            <button class="gray">Close</button>
        </form>
    {% else %}
        <span class="mini">Controls disabled</span>
    {% endif %}
    </div>
</div>
{% else %}
<div class="empty">No Level 2 barriers loaded.</div>
{% endfor %}
</div>
</div>

</div>

<div class="card" style="margin-top:10px">
<h2>Live Vehicles · All 3 Zones</h2>
<div class="scroll">
<table>
<tr>
    <th>Plate</th><th>Type</th><th>Status</th><th>Current Zone</th>
    <th>Route</th><th>Spot</th><th>Entry</th><th>Exit</th>
    <th>Payment</th><th>Operator Action</th>
</tr>
{% for c in cars %}
<tr>
    <td><b>{{c.plate}}</b></td>
    <td>{{c.car_type or '-'}}</td>
    <td>
        <span class="status {{c.status_class}}">{{c.status}}</span>
    </td>
    <td><b>{{c.current_zone or '-'}}</b></td>
    <td class="route">{{c.route}}</td>
    <td>{{c.actual_spot or c.assigned_spot or '-'}}</td>
    <td>{{c.entry_spot or '-'}}<div class="mini">{{c.entry_time[11:19] if c.entry_time else '-'}}</div></td>
    <td>{{c.exit_spot or '-'}}<div class="mini">{{c.exit_arrival_time[11:19] if c.exit_arrival_time else '-'}}</div></td>
    <td class="{{'good' if c.payment_status=='PAID' else ('bad' if c.payment_status in ['INVALID','CHARGE_ERROR'] else '')}}">
        {{c.payment_status}}
    </td>
    <td>
        {% if c.status == 'PARKED' %}
        <form method="post" action="/control/car/{{c.plate}}/exit">
            <button>Send to Exit</button>
        </form>
        {% endif %}

        {% if c.status in ['PAYMENT_HOLD','PAYMENT_PENDING','AT_EXIT'] and c.payment_status != 'PAID' %}
        <form method="post" action="/control/payment/{{c.plate}}/retry">
            <button class="danger">Retry Payment</button>
        </form>
        {% endif %}
    </td>
</tr>
{% else %}
<tr><td colspan="10" class="empty">No vehicles recorded yet.</td></tr>
{% endfor %}
</table>
</div>
</div>

<div class="two">

<div class="card">
<h2>Exit / Payment Flow</h2>
<div class="compact-scroll">
<table>
<tr><th>Plate</th><th>Zone</th><th>Exit Sensor</th><th>Wait</th><th>Payment</th><th>Status</th></tr>
{% for c in exits %}
<tr>
    <td><b>{{c.plate}}</b></td>
    <td>{{c.current_zone or c.exit_zone or '-'}}</td>
    <td>{{c.exit_spot or '-'}}</td>
    <td>{{c.wait}}</td>
    <td class="{{'good' if c.payment_status=='PAID' else 'warn'}}">{{c.payment_status}}</td>
    <td>{{c.status}}</td>
</tr>
{% else %}
<tr><td colspan="6" class="empty">No car currently in the exit/payment flow.</td></tr>
{% endfor %}
</table>
</div>
<div class="mini" style="margin-top:7px">
Paid-only interlock is enforced by the shared Level 2 controller, not just hidden in this UI.
</div>
</div>

<div class="card">
<h2>Last 3 Login Attempts</h2>
{% for x in logins %}
<div class="loginrow">
    <b>{{x.username}}</b> ·
    <span class="{{'good' if x.success else 'bad'}}">
        {{'SUCCESS' if x.success else 'FAILED'}}
    </span>
    <div class="mini">{{x.attempted_at}} · {{x.ip}}</div>
</div>
{% else %}
<div class="empty">No login attempts recorded yet.</div>
{% endfor %}
</div>

</div>

<div class="card" style="margin-top:10px">
<h2>90-Bay Level 2 Parking Map</h2>

{% for z in zones %}
<div class="map-zone">
    <div class="map-head">
        <div>
            <b>{{z.name}}</b>
            <span class="mini"> · {{z.total}} bays · {{z.occupied}} occupied · {{z.reserved}} reserved · {{z.free}} available</span>
        </div>
        <div class="mini">{{z.entry}} → {{z.exit}}</div>
    </div>

    <div class="spots">
    {% for s in z.spots %}
        <div class="spot {{s.css}}" title="{{s.name}} · {{s.type}} · {{s.state}}">
            <b>{{s.name}}</b>
            <span class="type">{{s.type_short}}</span>
        </div>
    {% endfor %}
    </div>
</div>
{% endfor %}

<div class="legend">
    <span><i class="dot free"></i>Free</span>
    <span><i class="dot busy"></i>Occupied</span>
    <span><i class="dot res"></i>Reserved/In-flight</span>
    <span><i class="dot bad"></i>Broken</span>
    <span><i class="dot maint"></i>Maintenance</span>
    <span>Type: A=Any · E=Electric · ACC=Accessible</span>
</div>
</div>

<details data-panel="flow-help">
<summary>Level 2 routing reference</summary>
<div class="two">
{% for z in zones %}
<div class="zone-card">
    <b>{{z.name}}</b>
    <div class="route" style="margin-top:5px">
        Entry sensor: <b>{{z.entry}}</b><br>
        Real parking bays: <b>{{z.total}}</b><br>
        Exit sensor: <b>{{z.exit}}</b><br>
        Current moving cars: <b>{{z.moving}}</b>
    </div>
</div>
{% endfor %}
</div>
</details>

</div>

<script>
(function(){
    const stateKey = "parkmind-operator-v2-panels";
    let panelState = {};
    try { panelState = JSON.parse(sessionStorage.getItem(stateKey) || "{}"); } catch(e) {}

    document.querySelectorAll("details[data-panel]").forEach((panel)=>{
        if (Object.prototype.hasOwnProperty.call(panelState,panel.dataset.panel)){
            panel.open = !!panelState[panel.dataset.panel];
        }
        panel.addEventListener("toggle",()=>{
            panelState[panel.dataset.panel]=panel.open;
            sessionStorage.setItem(stateKey,JSON.stringify(panelState));
        });
    });

    const y = parseInt(sessionStorage.getItem("parkmind-operator-scroll") || "0",10);
    if (y > 0) window.scrollTo(0,y);

    setTimeout(()=>{
        sessionStorage.setItem("parkmind-operator-scroll",String(window.scrollY||0));
        window.location.reload();
    },3500);
})();
</script>

</body>
</html>
"""


def db():
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def parse_time(value):
    try:
        return datetime.strptime(str(value)[:19], "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def fmt_duration(seconds):
    seconds = max(0, int(seconds or 0))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m:02d}m"
    if m:
        return f"{m}m {s:02d}s"
    return f"{s}s"


def danger_truthy(value):
    return str(value or "").strip().lower() in {
        "1", "true", "mid", "moderate", "high",
        "critical", "danger", "unsafe", "yes"
    }


def natural_key(value):
    value = str(value or "")
    match = re.search(r"(\d+)$", value)
    if match:
        return (value[:match.start()].lower(), int(match.group(1)))
    return (value.lower(), -1)


def parse_raw(raw_json):
    try:
        return json.loads(raw_json or "{}")
    except Exception:
        return {}


def car_zone(car, spot_zone):
    # Physical/assigned parking bay is strongest for operational location.
    actual = car.get("actual_spot")
    assigned = car.get("assigned_spot")
    if actual and actual in spot_zone:
        return spot_zone[actual]
    if assigned and assigned in spot_zone:
        return spot_zone[assigned]

    # Once routed toward an exit, the exit zone is authoritative.
    if car.get("exit_zone"):
        return car.get("exit_zone")

    return car.get("entry_zone") or ""


def car_status_class(status):
    status = str(status or "")
    if status in {
        "PAYMENT_HOLD", "UNREGISTERED_EXIT", "UNREGISTERED_PARKED",
        "ENTRY_HOLD", "NO_SAFE_SPACE", "PAID_EXIT_HOLD"
    }:
        return "bad"
    if status in {"PARKED", "PAID", "LEFT"}:
        return "good"
    if status in {
        "TO_SPOT", "WAITING_ENTRY", "TO_EXIT",
        "AT_EXIT", "PAYMENT_PENDING", "PAID_WAITING_GATE"
    }:
        return "warn"
    return "cyan"


@operator_bp.route("/")
def dashboard():
    if not session.get("user"):
        return redirect("/login")

    if session.get("role") != "Operator":
        return redirect("/")

    conn = db()

    state = conn.execute(
        "SELECT value FROM system_state WHERE key='last_simulator_time'"
    ).fetchone()
    sim_time = state["value"] if state else ""
    now = parse_time(sim_time)

    # ------------------------------------------------------------
    # REAL parking bays only — exclude EntrySpot/ExitSpot/LeaveParking.
    # ------------------------------------------------------------
    component_spots = [
        dict(r)
        for r in conn.execute(
            """SELECT *
               FROM components
               WHERE kind='spot'
               ORDER BY zone,name"""
        ).fetchall()
    ]

    reservation_rows = [
        dict(r)
        for r in conn.execute(
            "SELECT * FROM reservations"
        ).fetchall()
    ]
    reserved_by_spot = {
        r["spot"]: r["plate"] for r in reservation_rows
    }

    real_spots = []
    spot_zone = {}

    for row in component_spots:
        raw = parse_raw(row.get("raw_json"))
        if str(raw.get("purpose") or "") != "Park":
            continue

        name = row["name"]
        zone = row.get("zone") or "UNASSIGNED"
        spot_zone[name] = zone

        ptype = str(raw.get("parkingForCarType") or "Any")
        lower_type = ptype.lower()
        if lower_type == "electric":
            type_short = "E"
        elif lower_type == "accessible":
            type_short = "ACC"
        else:
            type_short = "A"

        occupied = str(row.get("state") or "").lower() == "occupied"
        reserved = name in reserved_by_spot and not occupied
        broken = bool(row.get("broken"))
        maintenance = bool(row.get("under_maintenance"))

        css = ""
        if broken:
            css = "broken"
        elif maintenance:
            css = "maint"
        elif occupied:
            css = "busy"
        elif reserved:
            css = "res"

        real_spots.append({
            "name": name,
            "zone": zone,
            "state": row.get("state") or "Unknown",
            "occupied": occupied,
            "reserved": reserved,
            "broken": broken,
            "maintenance": maintenance,
            "type": ptype,
            "type_short": type_short,
            "css": css,
        })

    # ------------------------------------------------------------
    # Cars — operational fields only.
    # ------------------------------------------------------------
    cars = [
        dict(r)
        for r in conn.execute(
            """SELECT *
               FROM cars
               WHERE departure_time IS NULL
                  OR status IN('ESCAPED_UNPAID','UNREGISTERED_EXIT')
               ORDER BY
                 CASE
                   WHEN status IN(
                     'PAYMENT_HOLD','UNREGISTERED_EXIT','UNREGISTERED_PARKED',
                     'ENTRY_HOLD','NO_SAFE_SPACE','PAID_EXIT_HOLD'
                   ) THEN 0
                   WHEN status IN(
                     'AT_EXIT','PAYMENT_PENDING','PAID_WAITING_GATE','TO_EXIT'
                   ) THEN 1
                   ELSE 2
                 END,
                 COALESCE(entry_time,'') DESC
               LIMIT 50"""
        ).fetchall()
    ]

    for car in cars:
        zone = car_zone(car, spot_zone)
        car["current_zone"] = zone
        car["status_class"] = car_status_class(car.get("status"))

        entry = car.get("entry_spot") or "?"
        target = car.get("actual_spot") or car.get("assigned_spot") or "?"
        exit_spot = car.get("exit_spot") or ZONE_FLOW.get(zone, {}).get("exit") or "?"

        if car.get("status") in {
            "TO_EXIT", "AT_EXIT", "PAYMENT_PENDING",
            "PAYMENT_HOLD", "PAID_WAITING_GATE", "PAID_EXIT_HOLD"
        }:
            car["route"] = f"{target} → {exit_spot}"
        elif car.get("status") == "PARKED":
            car["route"] = f"{entry} → {target}"
        else:
            car["route"] = f"{entry} → {target}"

    # ------------------------------------------------------------
    # Zones + real API CO + fan state.
    # ------------------------------------------------------------
    zone_rows = [
        dict(r)
        for r in conn.execute(
            "SELECT * FROM zones ORDER BY name"
        ).fetchall()
    ]
    zone_db = {r["name"]: r for r in zone_rows}

    fan_rows = [
        dict(r)
        for r in conn.execute(
            """SELECT *
               FROM components
               WHERE kind='fan'
               ORDER BY zone,name"""
        ).fetchall()
    ]

    zones = []
    for zone_name in ("ZONE1", "ZONE2", "ZONE3"):
        zspots = sorted(
            [s for s in real_spots if s["zone"] == zone_name],
            key=lambda x: natural_key(x["name"])
        )

        occupied = sum(1 for s in zspots if s["occupied"])
        reserved = sum(1 for s in zspots if s["reserved"])
        unavailable = sum(
            1 for s in zspots
            if (s["broken"] or s["maintenance"]) and not s["occupied"]
        )
        free = max(0, len(zspots) - occupied - reserved - unavailable)

        zone_cars = [
            c for c in cars
            if c.get("current_zone") == zone_name
            and c.get("status") not in ("LEFT", "ESCAPED_UNPAID")
        ]
        moving = sum(
            1 for c in zone_cars
            if c.get("status") in {
                "WAITING_ENTRY", "TO_SPOT", "REROUTING",
                "TO_EXIT", "AT_EXIT", "PAYMENT_PENDING",
                "PAID_WAITING_GATE"
            }
        )

        zdb = zone_db.get(zone_name, {})
        risk = zdb.get("danger") or "Safe"
        co_level = zdb.get("co_level")
        is_danger = danger_truthy(risk)

        fans_here = [f for f in fan_rows if f.get("zone") == zone_name]
        if fans_here:
            fan_text = ", ".join(
                f"{f['name']}={f.get('state') or '?'}"
                + (" BROKEN" if f.get("broken") else "")
                + (" MAINT" if f.get("under_maintenance") else "")
                for f in fans_here
            )
        else:
            fan_text = "No fan data"

        flow = ZONE_FLOW.get(zone_name, {})

        zones.append({
            "name": zone_name,
            "total": len(zspots),
            "occupied": occupied,
            "reserved": reserved,
            "free": free,
            "unavailable": unavailable,
            "active_cars": len(zone_cars),
            "moving": moving,
            "percent": round((occupied / len(zspots)) * 100) if zspots else 0,
            "risk": risk,
            "co_level": co_level,
            "is_danger": is_danger,
            "fans": fan_text,
            "entry": flow.get("entry", "-"),
            "exit": flow.get("exit", "-"),
            "spots": zspots,
        })

    # ------------------------------------------------------------
    # Alerts — operational only, no financial/maintenance planning.
    # ------------------------------------------------------------
    alerts = [
        dict(r)
        for r in conn.execute(
            """SELECT *
               FROM alerts
               WHERE active=1
                 AND alert_type NOT IN(
                   'SIMULATOR PENALTY',
                   'PREVENTIVE MAINTENANCE DUE',
                   'SIMULATOR MAINTENANCE REQUIRED',
                   'REPAIR WAITING FOR SAFE WINDOW'
                 )
               ORDER BY
                 CASE severity
                   WHEN 'CRITICAL' THEN 0
                   WHEN 'HIGH' THEN 1
                   ELSE 2
                 END,
                 id DESC
               LIMIT 20"""
        ).fetchall()
    ]

    # ------------------------------------------------------------
    # Exit/payment operational queue.
    # ------------------------------------------------------------
    exits = [
        dict(r)
        for r in conn.execute(
            """SELECT *
               FROM cars
               WHERE departure_time IS NULL
                 AND status IN(
                   'TO_EXIT','AT_EXIT','PAYMENT_PENDING','PAYMENT_HOLD',
                   'PAID_WAITING_GATE','PAID_EXIT_HOLD','UNREGISTERED_EXIT'
                 )
               ORDER BY COALESCE(exit_arrival_time,entry_time,'') ASC"""
        ).fetchall()
    ]

    for car in exits:
        car["current_zone"] = car_zone(car, spot_zone)
        start = parse_time(car.get("exit_arrival_time"))
        car["wait"] = (
            fmt_duration((now - start).total_seconds())
            if now and start else "-"
        )

    # ------------------------------------------------------------
    # Gates — all 7 Level 2 barriers.
    # ------------------------------------------------------------
    gates = [
        dict(r)
        for r in conn.execute(
            """SELECT c.*,
                      COALESCE(g.role,'unknown') AS role,
                      COALESCE(g.source,'') AS role_source
               FROM components c
               LEFT JOIN gate_roles g ON g.name=c.name
               WHERE c.kind='gate'
               ORDER BY
                 CASE
                   WHEN c.zone='ZONE1' THEN 1
                   WHEN c.zone='ZONE2' THEN 2
                   WHEN c.zone='ZONE3' THEN 3
                   ELSE 4
                 END,
                 c.name"""
        ).fetchall()
    ]

    logins = [
        dict(r)
        for r in conn.execute(
            "SELECT * FROM login_attempts ORDER BY id DESC LIMIT 3"
        ).fetchall()
    ]

    conn.close()

    capacity = {
        "total": len(real_spots),
        "occupied": sum(1 for s in real_spots if s["occupied"]),
        "reserved": sum(1 for s in real_spots if s["reserved"]),
    }

    active_cars = [
        c for c in cars
        if c.get("departure_time") is None
        and c.get("status") not in ("LEFT", "ESCAPED_UNPAID")
    ]

    moving_statuses = {
        "WAITING_ENTRY", "TO_SPOT", "REROUTING",
        "TO_EXIT", "AT_EXIT", "PAYMENT_PENDING",
        "PAID_WAITING_GATE"
    }

    ops = {
        "active": len(active_cars),
        "moving": sum(
            1 for c in active_cars
            if c.get("status") in moving_statuses
        ),
        "parked": sum(
            1 for c in active_cars
            if c.get("status") == "PARKED"
        ),
        "at_exit": len(exits),
        "alerts": len(alerts),
        "unsafe_zones": sum(1 for z in zones if z["is_danger"]),
    }

    return render_template_string(
        HTML,
        user=session.get("user") or "operator",
        sim_time=sim_time,
        capacity=capacity,
        ops=ops,
        zones=zones,
        cars=cars,
        exits=exits,
        alerts=alerts,
        gates=gates,
        logins=logins,
    )
