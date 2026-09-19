from flask import Blueprint, render_template_string, session, redirect
import sqlite3
from pathlib import Path
import os

maintenance_bp = Blueprint("maintenance", __name__, url_prefix="/maintenance")
APP_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.getenv("PARKMIND_LEVEL2_DB", str(APP_DIR / "parkmind_level2.db")))

CYCLE_LIMITS={"spot":int(os.getenv("PARKMIND_SPOT_MAINT_CYCLES","20")),"gate":int(os.getenv("PARKMIND_GATE_MAINT_CYCLES","30")),"fan":int(os.getenv("PARKMIND_FAN_MAINT_CYCLES","20")),"light":int(os.getenv("PARKMIND_LIGHT_MAINT_CYCLES","50"))}
RUNTIME_LIMITS={"fan":int(os.getenv("PARKMIND_FAN_MAINT_RUNTIME","1800")),"light":int(os.getenv("PARKMIND_LIGHT_MAINT_RUNTIME","3600"))}

HTML = """
<!doctype html><html><head><title>PARKMIND — Maintenance</title>
<style>
*{box-sizing:border-box}body{margin:0;background:#07111f;color:#e5e7eb;font-family:'Segoe UI',Arial}header{height:64px;padding:0 20px;background:#0e1a2c;border-bottom:1px solid #26364e;display:flex;align-items:center;justify-content:space-between}.brand{font-size:21px;font-weight:900;color:#67e8f9}.mini{font-size:10px;color:#94a3b8}a{color:#67e8f9;text-decoration:none}.page{max-width:1500px;margin:auto;padding:14px}.grid{display:grid;grid-template-columns:1fr 1fr;gap:10px}.card{background:#101b2d;border:1px solid #26364e;border-radius:13px;padding:12px;margin-bottom:10px}h2{margin:0 0 9px;font-size:13px;text-transform:uppercase;color:#cbd5e1}.bad{color:#f87171}.warn{color:#fbbf24}.good{color:#86efac}.health{height:7px;background:#1e293b;border-radius:5px;overflow:hidden}.health i{display:block;height:100%;background:#38bdf8}table{width:100%;border-collapse:collapse;font-size:11px}th,td{padding:7px;border-bottom:1px solid #26364e;text-align:left}th{font-size:9px;color:#94a3b8;text-transform:uppercase}button{padding:5px 8px;border:0;border-radius:7px;background:#0284c7;color:#fff;font-size:10px;cursor:pointer}button.danger{background:#b91c1c}button.gray{background:#475569}form{display:inline}.due{background:#27170b}.broken{background:#2b1116}@media(max-width:900px){.grid{grid-template-columns:1fr}}
</style></head>
<body><header><div><div class="brand">PARKMIND · Maintenance</div><div class="mini">Component health, preventive service, CO equipment and energy control</div></div><div><span class="mini">{{sim_time or 'Waiting for simulator time'}}</span> &nbsp; <a href="/logout">Logout</a></div></header>
<div class="page">

<div class="card">
<h2>Preventive Maintenance Queue</h2>
<table><tr><th>Component</th><th>Kind</th><th>Zone</th><th>Health</th><th>Cycles</th><th>Runtime</th><th>State</th><th>Action</th></tr>
{% for c in due %}
<tr class="{{'broken' if c.broken else 'due'}}">
<td><b>{{c.name}}</b></td><td>{{c.kind}}</td><td>{{c.zone or '-'}}</td>
<td><b class="{{'bad' if c.health<=20 else 'warn'}}">{{c.health}}%</b><div class="health"><i style="width:{{c.health}}%"></i></div></td>
<td>{{c.cycles}}</td><td>{{c.runtime}}</td><td>{{c.state}} {% if c.broken %}<span class="bad">BROKEN</span>{% endif %}</td>
<td><form method="post" action="/control/repair/{{c.kind}}/{{c.name}}"><button class="danger">Repair</button></form></td>
</tr>
{% else %}<tr><td colspan="8">No component currently requires preventive or corrective maintenance.</td></tr>{% endfor %}
</table>
</div>

<div class="grid">
<div class="card">
<h2>CO Zones & Exhaust Fans</h2>
{% for z in zones %}
<div style="padding:8px;border-bottom:1px solid #26364e">
<div><b>{{z.name}}</b> · <span class="{{'bad' if z.danger else 'good'}}">{{'DANGER' if z.danger else 'SAFE'}}</span></div>
<div class="mini">CO={{z.co_level if z.co_level is not none else 'No simulator reading'}} · {{z.last_update or '-'}}</div>
{% for f in z.fans %}
<div style="margin-top:5px"><b>{{f.name}}</b> {{f.state}}
<form method="post" action="/control/fan/{{f.name}}/on"><button>On</button></form>
<form method="post" action="/control/fan/{{f.name}}/off"><button class="gray">Off</button></form>
<form method="post" action="/control/repair/fan/{{f.name}}"><button class="danger">Repair</button></form>
</div>
{% endfor %}
</div>
{% endfor %}
</div>

<div class="card">
<h2>Lighting / Energy</h2>
<div class="mini" style="margin-bottom:7px">Automatic policy uses simulator ServerDateTime: daytime lights OFF; at night, only active zones need lighting.</div>
<table><tr><th>Light</th><th>Zone</th><th>State</th><th>Runtime</th><th>Cycles</th><th>Action</th></tr>
{% for l in lights %}
<tr><td><b>{{l.name}}</b></td><td>{{l.zone or '-'}}</td><td>{{l.state}}</td><td>{{l.runtime}}</td><td>{{l.cycles}}</td>
<td><form method="post" action="/control/light/{{l.name}}/on"><button>On</button></form><form method="post" action="/control/light/{{l.name}}/off"><button class="gray">Off</button></form><form method="post" action="/control/repair/light/{{l.name}}"><button class="danger">Repair</button></form></td></tr>
{% endfor %}
</table>
</div>
</div>

<div class="card">
<h2>All Components</h2>
<table><tr><th>Name</th><th>Kind</th><th>Zone</th><th>State</th><th>Health</th><th>Broken</th><th>Maintenance</th><th>Cycles</th><th>Runtime</th></tr>
{% for c in components %}
<tr><td><b>{{c.name}}</b></td><td>{{c.kind}}</td><td>{{c.zone or '-'}}</td><td>{{c.state}}</td><td>{{c.health}}%</td><td>{{'YES' if c.broken else 'No'}}</td><td>{{'YES' if c.under_maintenance else 'No'}}</td><td>{{c.cycles}}</td><td>{{c.runtime}}</td></tr>
{% endfor %}
</table>
<form method="post" action="/control/sync" style="margin-top:10px"><button>Refresh Topology from Simulator</button></form>
</div>

<div class="card">
<h2>Last 3 Login Attempts</h2>
<table><tr><th>Time</th><th>Name</th><th>Result</th><th>IP</th></tr>
{% for x in logins %}<tr><td>{{x.attempted_at}}</td><td>{{x.username}}</td><td class="{{'good' if x.success else 'bad'}}">{{'SUCCESS' if x.success else 'FAILED'}}</td><td>{{x.ip}}</td></tr>{% endfor %}
</table>
</div>

</div><script>setTimeout(()=>window.location.reload(),8000);</script></body></html>
"""

def db():
    c=sqlite3.connect(str(DB_PATH),timeout=10);c.row_factory=sqlite3.Row;return c

def danger_truthy(v):return str(v).strip().lower() in ("1","true","danger","high","critical","yes","unsafe")

def duration(sec):
    sec=max(0,int(sec or 0));h,rem=divmod(sec,3600);m,s=divmod(rem,60)
    return f"{h}h {m:02d}m" if h else (f"{m}m {s:02d}s" if m else f"{s}s")

def health(c):
    if c["broken"]:return 0
    if c["under_maintenance"]:return 20
    wear=0.0
    lim=CYCLE_LIMITS.get(c["kind"])
    if lim:wear=max(wear,(c["cycles"] or 0)/lim)
    rlim=RUNTIME_LIMITS.get(c["kind"])
    if rlim:wear=max(wear,(c["runtime_seconds"] or 0)/rlim)
    return max(0,round(100*(1-min(wear,1))))

@maintenance_bp.route("/")
def dashboard():
    if not session.get("user"):return redirect("/login")
    if session.get("role")!="Maintenance":return redirect("/")
    conn=db()
    state=conn.execute("SELECT value FROM system_state WHERE key='last_simulator_time'").fetchone();sim_time=state["value"] if state else ""
    components=[dict(r) for r in conn.execute("SELECT * FROM components ORDER BY kind,zone,name").fetchall()]
    for c in components:c["health"]=health(c);c["runtime"]=duration(c["runtime_seconds"])
    due=[c for c in components if c["broken"] or c["under_maintenance"] or c["health"]<=20]

    zone_rows=[dict(r) for r in conn.execute("SELECT * FROM zones ORDER BY name").fetchall()]
    fans=[dict(r) for r in conn.execute("SELECT * FROM components WHERE kind='fan' ORDER BY zone,name").fetchall()]
    zones=[]
    for z in zone_rows:
        z["danger"]=danger_truthy(z["danger"]);z["fans"]=[f for f in fans if f["zone"]==z["name"]];zones.append(z)

    lights=[dict(r) for r in conn.execute("SELECT * FROM components WHERE kind='light' ORDER BY zone,name").fetchall()]
    for l in lights:l["runtime"]=duration(l["runtime_seconds"])
    logins=[dict(r) for r in conn.execute("SELECT * FROM login_attempts ORDER BY id DESC LIMIT 3").fetchall()]
    conn.close()
    return render_template_string(HTML,sim_time=sim_time,components=components,due=due,zones=zones,lights=lights,logins=logins)
