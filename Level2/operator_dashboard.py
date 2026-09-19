from flask import Blueprint, render_template_string, session, redirect
import sqlite3
from pathlib import Path
import os
from datetime import datetime

operator_bp = Blueprint("operator", __name__, url_prefix="/operator")
APP_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.getenv("PARKMIND_LEVEL2_DB", str(APP_DIR / "parkmind_level2.db")))

HTML = """
<!doctype html>
<html><head><title>PARKMIND — Operator</title>
<style>
*{box-sizing:border-box}body{margin:0;background:#07111f;color:#e5e7eb;font-family:'Segoe UI',Arial}
header{height:64px;padding:0 20px;background:#0e1a2c;border-bottom:1px solid #26364e;display:flex;align-items:center;justify-content:space-between}
.brand{font-size:21px;font-weight:900;color:#67e8f9}.sub,.mini{font-size:10px;color:#94a3b8}
a{color:#67e8f9;text-decoration:none}.page{max-width:1450px;margin:auto;padding:14px}
.banner{padding:9px 11px;border-radius:9px;background:#082f49;border:1px solid #0e7490;color:#bae6fd;font-size:11px;margin-bottom:10px}
.grid{display:grid;grid-template-columns:1.15fr .85fr;gap:10px}.two{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:10px}
.card{background:#101b2d;border:1px solid #26364e;border-radius:13px;padding:12px;min-width:0}
h2{margin:0 0 9px;font-size:13px;text-transform:uppercase;letter-spacing:.6px;color:#cbd5e1}
.alert{padding:9px;background:#2b1116;border-left:4px solid #ef4444;border-radius:8px;margin:6px 0;display:grid;grid-template-columns:100px 90px 80px 1fr;gap:8px;font-size:10px;align-items:center}
.crit{color:#fca5a5;font-weight:800}.good{color:#86efac}.warn{color:#fbbf24}.bad{color:#f87171}
table{width:100%;border-collapse:collapse;font-size:11px}th,td{padding:7px;border-bottom:1px solid #26364e;text-align:left;white-space:nowrap}th{font-size:9px;color:#94a3b8;text-transform:uppercase}
.scroll{max-height:315px;overflow:auto}.status{padding:3px 6px;border-radius:999px;background:#1e293b;font-size:9px;font-weight:700}
button{border:0;border-radius:7px;padding:5px 8px;background:#0284c7;color:#fff;font-size:10px;cursor:pointer}button.danger{background:#b91c1c}button.gray{background:#475569}
form{display:inline}.zone{padding:9px;border:1px solid #26364e;background:#0a1424;border-radius:9px;margin-bottom:7px}.zone.danger{border-color:#b91c1c;background:#271117}
.gate{display:flex;justify-content:space-between;align-items:center;padding:7px 0;border-bottom:1px solid #26364e}
.loginrow{font-size:10px;padding:5px 0;border-bottom:1px solid #26364e}
.empty{padding:20px;color:#64748b;text-align:center;font-size:11px}
@media(max-width:900px){.grid,.two{grid-template-columns:1fr}.alert{grid-template-columns:90px 80px 1fr}.alert .hide-mobile{display:none}}
</style></head>
<body>
<header>
<div><div class="brand">PARKMIND · Operator</div><div class="sub">Live incident response and vehicle flow</div></div>
<div><span class="mini">{{sim_time or 'Waiting for simulator time'}}</span> &nbsp; <a href="/logout">Logout</a></div>
</header>
<div class="page">
<div class="banner">
This screen intentionally excludes revenue, financial reports, penalty totals and maintenance planning.
The operator sees only what is needed to keep people and vehicles moving safely.
</div>

<div class="grid">
<div class="card">
<h2>🚨 Critical Operational Alerts</h2>
<div class="scroll">
{% for a in alerts %}
<div class="alert">
<div><div class="crit">{{a.alert_type}}</div><div class="mini">{{a.severity}}</div></div>
<div><b>{{a.plate or a.component or '-'}}</b><div class="mini">{{a.zone or '-'}}</div></div>
<div class="hide-mobile">{{a.simulator_time[11:19] if a.simulator_time else '-'}}</div>
<div>{{a.reason}}</div>
</div>
{% else %}
<div class="empty">No active critical operational alerts.</div>
{% endfor %}
</div>
</div>

<div class="card">
<h2>Exit & Payment Queue</h2>
<div class="scroll">
<table>
<tr><th>Plate</th><th>Exit</th><th>At Exit</th><th>Wait</th><th>Payment</th><th>Status</th><th>Action</th></tr>
{% for c in exits %}
<tr>
<td><b>{{c.plate}}</b></td>
<td>{{c.exit_spot or '-'}}</td>
<td>{{c.exit_arrival_time[11:19] if c.exit_arrival_time else '-'}}</td>
<td>{{c.wait}}</td>
<td class="{{'good' if c.payment_status=='PAID' else 'bad'}}">{{c.payment_status}}</td>
<td><span class="status">{{c.status}}</span></td>
<td>
{% if c.status in ['PAYMENT_HOLD','PAYMENT_PENDING','AT_EXIT'] and c.payment_status!='PAID' %}
<form method="post" action="/control/payment/{{c.plate}}/retry"><button class="danger">Retry Pay</button></form>
{% endif %}
</td>
</tr>
{% else %}
<tr><td colspan="7" class="empty">No vehicles waiting at exit.</td></tr>
{% endfor %}
</table>
</div>
<div class="mini" style="margin-top:7px">Rule: a vehicle is never added to gate release until payment status is PAID.</div>
</div>
</div>

<div class="two">
<div class="card">
<h2>CO Safety & Ventilation</h2>
{% for z in zones %}
<div class="zone {{'danger' if z.is_danger else ''}}">
<div style="display:flex;justify-content:space-between"><b>{{z.name}}</b><span class="{{'bad' if z.is_danger else 'good'}}">{{'DANGER' if z.is_danger else 'SAFE'}}</span></div>
<div class="mini">CO: {{z.co_level if z.co_level is not none else 'No simulator reading yet'}} · last simulator update: {{z.last_update or '-'}}</div>
<div class="mini">Fans: {{z.fans}}</div>
</div>
{% else %}
<div class="empty">Waiting for Level 2 zone data.</div>
{% endfor %}
</div>

<div class="card">
<h2>Gate Status</h2>
{% for g in gates %}
<div class="gate">
<div><b>{{g.name}}</b><div class="mini">{{g.zone or 'unassigned'}} · {{g.state}} · {{g.role|upper}} {% if g.broken %}· BROKEN{% endif %}</div></div>
<div>
{% if g.role != 'exit' %}
<form method="post" action="/control/gate/{{g.name}}/open"><button>Open</button></form>
{% else %}
<span class="mini">AUTO · PAID ONLY</span>
{% endif %}
<form method="post" action="/control/gate/{{g.name}}/close"><button class="gray">Close</button></form>
</div>
</div>
{% else %}
<div class="empty">No simulator barriers loaded.</div>
{% endfor %}
</div>
</div>

<div class="two">
<div class="card">
<h2>Vehicles Needing Attention</h2>
<table>
<tr><th>Plate</th><th>Status</th><th>Spot</th><th>Entry</th><th>Action</th></tr>
{% for c in attention %}
<tr>
<td><b>{{c.plate}}</b></td><td>{{c.status}}</td><td>{{c.actual_spot or c.assigned_spot or '-'}}</td>
<td>{{c.entry_time[11:19] if c.entry_time else '-'}}</td>
<td>
{% if c.status=='PARKED' %}
<form method="post" action="/control/car/{{c.plate}}/exit"><button>Send to Exit</button></form>
{% endif %}
</td>
</tr>
{% else %}
<tr><td colspan="5" class="empty">No vehicles require operator attention.</td></tr>
{% endfor %}
</table>
</div>

<div class="card">
<h2>Last 3 Login Attempts</h2>
{% for x in logins %}
<div class="loginrow">
<b>{{x.username}}</b> · <span class="{{'good' if x.success else 'bad'}}">{{'SUCCESS' if x.success else 'FAILED'}}</span>
<div class="mini">{{x.attempted_at}} · {{x.ip}}</div>
</div>
{% endfor %}
</div>
</div>
</div>
<script>setTimeout(()=>window.location.reload(),4000);</script>
</body></html>
"""

def db():
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.row_factory = sqlite3.Row
    return conn

def danger_truthy(value):
    return str(value).strip().lower() in ("1","true","danger","high","critical","yes","unsafe")

def fmt_duration(seconds):
    seconds=max(0,int(seconds or 0)); m,s=divmod(seconds,60); h,m=divmod(m,60)
    return f"{h}h {m:02d}m" if h else (f"{m}m {s:02d}s" if m else f"{s}s")

def parse_time(v):
    try:return datetime.strptime(str(v)[:19],"%Y-%m-%d %H:%M:%S")
    except:return None

@operator_bp.route("/")
def dashboard():
    if not session.get("user"):
        return redirect("/login")
    if session.get("role") != "Operator":
        return redirect("/")

    conn=db()
    state=conn.execute("SELECT value FROM system_state WHERE key='last_simulator_time'").fetchone()
    sim_time=state["value"] if state else ""

    alerts=[dict(r) for r in conn.execute(
        """SELECT * FROM alerts
           WHERE active=1
             AND alert_type NOT IN('SIMULATOR PENALTY','PREVENTIVE MAINTENANCE DUE')
           ORDER BY CASE severity WHEN 'CRITICAL' THEN 0 ELSE 1 END,id DESC
           LIMIT 20"""
    ).fetchall()]

    exits=[dict(r) for r in conn.execute(
        """SELECT * FROM cars
           WHERE departure_time IS NULL
             AND status IN('TO_EXIT','AT_EXIT','PAYMENT_PENDING','PAYMENT_HOLD',
                           'PAID_WAITING_GATE','UNREGISTERED_EXIT')
           ORDER BY COALESCE(exit_arrival_time,entry_time,'') ASC"""
    ).fetchall()]

    now=parse_time(sim_time)
    for c in exits:
        start=parse_time(c.get("exit_arrival_time"))
        c["wait"]=fmt_duration((now-start).total_seconds()) if now and start else "-"

    attention=[dict(r) for r in conn.execute(
        """SELECT * FROM cars
           WHERE departure_time IS NULL
             AND status IN('PAYMENT_HOLD','UNREGISTERED_EXIT','UNREGISTERED_PARKED',
                           'ENTRY_HOLD','NO_SAFE_SPACE','PARKED')
           ORDER BY COALESCE(entry_time,'') ASC
           LIMIT 20"""
    ).fetchall()]

    gates=[dict(r) for r in conn.execute(
        """SELECT c.*, COALESCE(g.role,'unknown') AS role
           FROM components c
           LEFT JOIN gate_roles g ON g.name=c.name
           WHERE c.kind='gate'
           ORDER BY c.zone,c.name"""
    ).fetchall()]

    zone_rows=[dict(r) for r in conn.execute(
        "SELECT * FROM zones ORDER BY name"
    ).fetchall()]
    fan_rows=[dict(r) for r in conn.execute(
        "SELECT * FROM components WHERE kind='fan' ORDER BY zone,name"
    ).fetchall()]

    zones=[]
    for z in zone_rows:
        fs=[f"{f['name']}={f['state']}" for f in fan_rows if f["zone"]==z["name"]]
        z["fans"]=", ".join(fs) if fs else "No fan mapped"
        z["is_danger"]=danger_truthy(z["danger"])
        zones.append(z)

    logins=[dict(r) for r in conn.execute(
        "SELECT * FROM login_attempts ORDER BY id DESC LIMIT 3"
    ).fetchall()]
    conn.close()

    return render_template_string(
        HTML,sim_time=sim_time,alerts=alerts,exits=exits,
        attention=attention,gates=gates,zones=zones,logins=logins
    )
