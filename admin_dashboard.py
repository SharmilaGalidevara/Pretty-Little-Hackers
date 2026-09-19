from flask import Blueprint, render_template_string, session, redirect, request
import sqlite3
from pathlib import Path
import os
from datetime import datetime, timedelta

admin_bp = Blueprint("admin", __name__, url_prefix="/admin")
APP_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.getenv("PARKMIND_LEVEL2_DB", str(APP_DIR / "parkmind_level2.db")))

DASH = """
<!doctype html>
<html><head><title>PARKMIND — Admin</title>
<style>
*{box-sizing:border-box}body{margin:0;background:#07111f;color:#e5e7eb;font-family:'Segoe UI',Arial}
header{height:64px;padding:0 20px;background:#0e1a2c;border-bottom:1px solid #26364e;display:flex;align-items:center;justify-content:space-between}
.brand{font-size:21px;font-weight:900;color:#67e8f9}.mini{font-size:10px;color:#94a3b8}a{color:#67e8f9;text-decoration:none}
.page{max-width:1500px;margin:auto;padding:14px}.kpis{display:grid;grid-template-columns:repeat(5,1fr);gap:9px;margin-bottom:10px}
.card,.kpi{background:#101b2d;border:1px solid #26364e;border-radius:13px;padding:12px}.kpi .v{font-size:24px;font-weight:900}.kpi .k{font-size:10px;color:#94a3b8;text-transform:uppercase}
.good{color:#86efac}.cyan{color:#67e8f9}.warn{color:#fbbf24}.bad{color:#f87171}
.grid{display:grid;grid-template-columns:1.35fr .65fr;gap:10px}.two{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:10px}
h2{margin:0 0 9px;font-size:13px;text-transform:uppercase;color:#cbd5e1}.bars{height:220px;display:flex;align-items:flex-end;gap:7px;padding:12px 6px 28px;border-bottom:1px solid #334155}
.col{flex:1;height:100%;display:flex;align-items:flex-end;justify-content:center;position:relative}.bar{width:70%;background:#0ea5e9;border-radius:5px 5px 0 0;min-height:2px}
.lbl{position:absolute;top:100%;font-size:9px;color:#94a3b8;margin-top:4px}.pie{width:170px;height:170px;border-radius:50%;margin:15px auto;border:8px solid #1e293b}
.legend{font-size:11px;line-height:1.8}.dot{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:5px}
table{width:100%;border-collapse:collapse;font-size:11px}th,td{padding:7px;border-bottom:1px solid #26364e;text-align:left}th{font-size:9px;color:#94a3b8;text-transform:uppercase}
.badge{padding:3px 6px;border-radius:999px;background:#1e293b;font-size:9px}.toolbar{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
button,input{padding:6px 8px;border-radius:7px}button{border:0;background:#0284c7;color:white;cursor:pointer}input{background:#08111f;color:white;border:1px solid #334155}
@media(max-width:900px){.kpis{grid-template-columns:repeat(2,1fr)}.grid,.two{grid-template-columns:1fr}}
</style></head>
<body>
<header>
<div><div class="brand">PARKMIND · Admin</div><div class="mini">Business, finance, compliance and system oversight</div></div>
<div class="toolbar"><span class="mini">{{sim_time or 'Waiting for simulator time'}}</span><a href="/admin/penalties">Penalties</a><a href="/admin/reports/daily">Daily Report</a><a href="/admin/audit">Audit</a><a href="/logout">Logout</a></div>
</header>
<div class="page">

<div class="kpis">
<div class="kpi"><div class="v good">{{'%.2f'|format(today.revenue)}}</div><div class="k">Revenue Today</div></div>
<div class="kpi"><div class="v cyan">{{'%.2f'|format(yesterday.revenue)}}</div><div class="k">Revenue Yesterday</div></div>
<div class="kpi"><div class="v {{'good' if delta is not none and delta>=0 else 'bad'}}">{{delta_text}}</div><div class="k">Today vs Yesterday</div></div>
<div class="kpi"><div class="v">{{today.paid_cars}}</div><div class="k">Paid Cars Today</div></div>
<div class="kpi"><div class="v warn">{{'%.2f'|format(today.avg_ticket)}}</div><div class="k">Average / Paid Car</div></div>
</div>

<div class="grid">
<div class="card">
<h2>7-Day Revenue — Simulator Dates</h2>
<div class="bars">
{% for x in daily %}
<div class="col" title="{{x.date}} · {{'%.2f'|format(x.revenue)}}">
<div class="bar" style="height:{{x.height}}%"></div>
<div class="lbl">{{x.short}}</div>
</div>
{% endfor %}
</div>
<div class="mini" style="margin-top:7px">Every bar is calculated from accepted payment_made events stored with simulator timestamps.</div>
</div>

<div class="card">
<h2>Today's Revenue Mix</h2>
{% if mix.total > 0 %}
<div class="pie" style="background:conic-gradient(#38bdf8 0 {{mix.parking_pct}}%, #a78bfa {{mix.parking_pct}}% 100%)"></div>
<div class="legend">
<span class="dot" style="background:#38bdf8"></span>Parking: <b>{{'%.2f'|format(mix.parking)}}</b> ({{mix.parking_pct}}%)<br>
<span class="dot" style="background:#a78bfa"></span>Charging: <b>{{'%.2f'|format(mix.charging)}}</b> ({{mix.charging_pct}}%)
</div>
{% else %}
<div class="mini">No accepted revenue yet for this simulator date.</div>
{% endif %}
</div>
</div>

<div class="two">
<div class="card">
<h2>Hourly Revenue Today</h2>
<div class="bars" style="height:160px">
{% for x in hourly %}
<div class="col" title="{{x.hour}} · {{'%.2f'|format(x.revenue)}}">
<div class="bar" style="height:{{x.height}}%"></div>
<div class="lbl">{{x.hour}}</div>
</div>
{% endfor %}
</div>
</div>

<div class="card">
<h2>System Overview</h2>
<table>
<tr><td>Active critical alerts</td><td><b>{{overview.alerts}}</b></td></tr>
<tr><td>Broken components</td><td><b>{{overview.broken}}</b></td></tr>
<tr><td>Maintenance due</td><td><b>{{overview.maintenance_due}}</b></td></tr>
<tr><td>Penalties today</td><td><b>{{overview.penalties}}</b></td></tr>
<tr><td>Failed logins today</td><td><b>{{overview.failed_logins}}</b></td></tr>
<tr><td>Signed webhook events today</td><td><b>{{overview.events}}</b></td></tr>
</table>
<form method="post" action="/control/sync" style="margin-top:10px"><button>Sync Simulator Topology</button></form>
</div>
</div>

<div class="two">
<div class="card">
<h2>Recent Accepted Payments</h2>
<table><tr><th>Simulator Time</th><th>Plate</th><th>Parking</th><th>Charging</th><th>Total Paid</th></tr>
{% for p in payments %}
<tr><td>{{p.payment_time}}</td><td><b>{{p.plate}}</b></td><td>{{'%.2f'|format(p.parking_cost or 0)}}</td><td>{{'%.2f'|format(p.charging_cost or 0)}}</td><td class="good"><b>{{'%.2f'|format(p.actual_paid or 0)}}</b></td></tr>
{% endfor %}
</table>
</div>
<div class="card">
<h2>Last 3 Login Attempts</h2>
<table><tr><th>Time</th><th>Name</th><th>Result</th><th>IP</th></tr>
{% for x in logins %}
<tr><td>{{x.attempted_at}}</td><td>{{x.username}}</td><td class="{{'good' if x.success else 'bad'}}">{{'SUCCESS' if x.success else 'FAILED'}}</td><td>{{x.ip}}</td></tr>
{% endfor %}
</table>
</div>
</div>

</div><script>setTimeout(()=>window.location.reload(),10000);</script></body></html>
"""

PENALTIES = """
<!doctype html><html><head><title>PARKMIND — Penalties</title>
<style>body{font-family:Segoe UI,Arial;background:#07111f;color:#e5e7eb;margin:0;padding:20px}a{color:#67e8f9}table{width:100%;border-collapse:collapse;background:#101b2d}th,td{padding:9px;border-bottom:1px solid #26364e;text-align:left}th{color:#94a3b8;font-size:11px}.bad{color:#f87171}</style></head>
<body><p><a href="/admin/">← Admin</a></p><h1>Dedicated Penalties Page</h1>
<table><tr><th>Simulator Time</th><th>Plate</th><th>Reason</th><th>Fine</th></tr>
{% for p in rows %}<tr><td>{{p.simulator_time}}</td><td>{{p.plate or '-'}}</td><td>{{p.reason}}</td><td class="bad">{{'%.2f'|format(p.fine_amount or 0)}}</td></tr>{% else %}<tr><td colspan="4">No simulator penalties recorded.</td></tr>{% endfor %}
</table></body></html>
"""

REPORT = """
<!doctype html><html><head><title>PARKMIND — Daily Report</title>
<style>body{font-family:Segoe UI,Arial;background:#07111f;color:#e5e7eb;margin:0;padding:20px}a{color:#67e8f9}.cards{display:grid;grid-template-columns:repeat(4,1fr);gap:10px}.c{background:#101b2d;border:1px solid #26364e;border-radius:10px;padding:12px}.v{font-size:22px;font-weight:800}.mini{font-size:10px;color:#94a3b8}table{width:100%;border-collapse:collapse;background:#101b2d;margin-top:12px}th,td{padding:8px;border-bottom:1px solid #26364e;text-align:left}input,button{padding:7px;border-radius:7px}input{background:#08111f;color:#fff;border:1px solid #334155}button{border:0;background:#0284c7;color:#fff}</style></head>
<body><p><a href="/admin/">← Admin</a></p><h1>Dynamic Daily Operations Report</h1>
<form><input type="date" name="date" value="{{date}}"><button>Load</button></form>
<div class="mini">All operational figures below use simulator timestamps for {{date}}.</div>
<div class="cards" style="margin-top:12px">
<div class="c"><div class="v">{{'%.2f'|format(summary.revenue)}}</div><div class="mini">Revenue</div></div>
<div class="c"><div class="v">{{summary.arrivals}}</div><div class="mini">Arrivals</div></div>
<div class="c"><div class="v">{{summary.departures}}</div><div class="mini">Departures</div></div>
<div class="c"><div class="v">{{summary.penalties}}</div><div class="mini">Penalties</div></div>
<div class="c"><div class="v">{{summary.co_incidents}}</div><div class="mini">CO incidents</div></div>
<div class="c"><div class="v">{{summary.broken}}</div><div class="mini">Broken events</div></div>
<div class="c"><div class="v">{{summary.repairs}}</div><div class="mini">Maintenance actions</div></div>
<div class="c"><div class="v">{{summary.failed_logins}}</div><div class="mini">Failed logins (system time)</div></div>
</div>
<h2>Important Alerts</h2><table><tr><th>Simulator Time</th><th>Type</th><th>Target</th><th>Reason</th></tr>
{% for a in alerts %}<tr><td>{{a.simulator_time}}</td><td>{{a.alert_type}}</td><td>{{a.plate or a.component or a.zone or '-'}}</td><td>{{a.reason}}</td></tr>{% else %}<tr><td colspan="4">No alerts for this date.</td></tr>{% endfor %}
</table></body></html>
"""

AUDIT = """
<!doctype html><html><head><title>PARKMIND — Audit</title>
<style>body{font-family:Segoe UI,Arial;background:#07111f;color:#e5e7eb;margin:0;padding:20px}a{color:#67e8f9}table{width:100%;border-collapse:collapse;background:#101b2d}th,td{padding:8px;border-bottom:1px solid #26364e;text-align:left;font-size:11px}th{color:#94a3b8}</style></head>
<body><p><a href="/admin/">← Admin</a></p><h1>Audit Log</h1><table><tr><th>System Time</th><th>Simulator Time</th><th>Actor</th><th>Action</th><th>Target</th><th>Detail</th><th>Result</th></tr>
{% for a in rows %}<tr><td>{{a.created_at}}</td><td>{{a.simulator_time or '-'}}</td><td>{{a.actor}}</td><td>{{a.action}}</td><td>{{a.target}}</td><td>{{a.detail}}</td><td>{{a.result}}</td></tr>{% endfor %}
</table></body></html>
"""

def db():
    c=sqlite3.connect(str(DB_PATH),timeout=10);c.row_factory=sqlite3.Row;return c

def current_sim_date(conn):
    row=conn.execute("SELECT value FROM system_state WHERE key='last_simulator_time'").fetchone()
    if row and row["value"]:
        return row["value"][:10],row["value"]
    row=conn.execute("SELECT MAX(payment_time) AS t FROM cars WHERE payment_time IS NOT NULL").fetchone()
    return ((row["t"][:10],row["t"]) if row and row["t"] else ("",""))

def date_stats(conn,date):
    if not date:return {"revenue":0.0,"paid_cars":0,"avg_ticket":0.0}
    row=conn.execute("""SELECT COALESCE(SUM(actual_paid),0) revenue,COUNT(*) paid_cars
                        FROM cars WHERE payment_status='PAID' AND substr(payment_time,1,10)=?""",(date,)).fetchone()
    rev=float(row["revenue"] or 0);n=int(row["paid_cars"] or 0)
    return {"revenue":rev,"paid_cars":n,"avg_ticket":rev/n if n else 0.0}

@admin_bp.route("/")
def dashboard():
    if not session.get("user"):return redirect("/login")
    if session.get("role")!="Admin":return redirect("/")
    conn=db();today_date,sim_time=current_sim_date(conn)
    if today_date:
        d=datetime.strptime(today_date,"%Y-%m-%d").date()
        yesterday_date=(d-timedelta(days=1)).isoformat()
    else:yesterday_date=""
    today=date_stats(conn,today_date);yesterday=date_stats(conn,yesterday_date)
    delta=None
    if yesterday["revenue"]>0:delta=(today["revenue"]-yesterday["revenue"])/yesterday["revenue"]*100
    delta_text=("N/A" if delta is None else f"{delta:+.1f}%")

    daily=[]
    if today_date:
        d=datetime.strptime(today_date,"%Y-%m-%d").date()
        vals=[]
        for i in range(6,-1,-1):
            ds=(d-timedelta(days=i)).isoformat();s=date_stats(conn,ds);vals.append((ds,s["revenue"]))
        peak=max([v for _,v in vals]+[0])
        daily=[{"date":ds,"short":ds[5:],"revenue":v,"height":round(v/peak*100,1) if peak else 0} for ds,v in vals]

    mixrow=conn.execute("""SELECT COALESCE(SUM(parking_cost),0) p,COALESCE(SUM(charging_cost),0) c
                           FROM cars WHERE payment_status='PAID' AND substr(payment_time,1,10)=?""",(today_date,)).fetchone()
    p=float(mixrow["p"] or 0);c=float(mixrow["c"] or 0);tot=p+c
    mix={"parking":p,"charging":c,"total":tot,"parking_pct":round(p/tot*100,1) if tot else 0,"charging_pct":round(c/tot*100,1) if tot else 0}

    hourly=[]
    peak=0;raw=[]
    for h in range(24):
        hh=f"{h:02d}"
        row=conn.execute("""SELECT COALESCE(SUM(actual_paid),0) r FROM cars
                            WHERE payment_status='PAID' AND substr(payment_time,1,10)=? AND substr(payment_time,12,2)=?""",(today_date,hh)).fetchone()
        v=float(row["r"] or 0);raw.append((hh,v));peak=max(peak,v)
    hourly=[{"hour":h,"revenue":v,"height":round(v/peak*100,1) if peak else 0} for h,v in raw]

    def count(sql,args=()):
        return int(conn.execute(sql,args).fetchone()[0] or 0)
    overview={
        "alerts":count("SELECT COUNT(*) FROM alerts WHERE active=1 AND severity='CRITICAL'"),
        "broken":count("SELECT COUNT(*) FROM components WHERE broken=1"),
        "maintenance_due":count("SELECT COUNT(*) FROM alerts WHERE active=1 AND alert_type='PREVENTIVE MAINTENANCE DUE'"),
        "penalties":count("SELECT COUNT(*) FROM penalties WHERE substr(simulator_time,1,10)=?",(today_date,)),
        "failed_logins":count("SELECT COUNT(*) FROM login_attempts WHERE success=0 AND substr(attempted_at,1,10)=?",(datetime.now().strftime("%Y-%m-%d"),)),
        "events":count("SELECT COUNT(*) FROM events WHERE signature_status='verified' AND substr(server_time,1,10)=?",(today_date,))
    }
    payments=[dict(r) for r in conn.execute("""SELECT plate,payment_time,parking_cost,charging_cost,actual_paid FROM cars
                                               WHERE payment_status='PAID' ORDER BY payment_time DESC LIMIT 10""").fetchall()]
    logins=[dict(r) for r in conn.execute("SELECT * FROM login_attempts ORDER BY id DESC LIMIT 3").fetchall()]
    conn.close()
    return render_template_string(DASH,sim_time=sim_time,today=today,yesterday=yesterday,delta=delta,delta_text=delta_text,daily=daily,mix=mix,hourly=hourly,overview=overview,payments=payments,logins=logins)

@admin_bp.route("/penalties")
def penalties():
    if session.get("role")!="Admin":return redirect("/")
    conn=db();rows=[dict(r) for r in conn.execute("SELECT * FROM penalties ORDER BY id DESC").fetchall()];conn.close()
    return render_template_string(PENALTIES,rows=rows)

@admin_bp.route("/reports/daily")
def daily_report():
    if session.get("role")!="Admin":return redirect("/")
    conn=db();default_date,_=current_sim_date(conn);date=request.args.get("date") or default_date
    def count(sql,args=()):return int(conn.execute(sql,args).fetchone()[0] or 0)
    revenue=float(conn.execute("SELECT COALESCE(SUM(actual_paid),0) FROM cars WHERE payment_status='PAID' AND substr(payment_time,1,10)=?",(date,)).fetchone()[0] or 0)
    summary={
        "revenue":revenue,
        "arrivals":count("SELECT COUNT(*) FROM cars WHERE substr(entry_time,1,10)=?",(date,)),
        "departures":count("SELECT COUNT(*) FROM cars WHERE substr(departure_time,1,10)=?",(date,)),
        "penalties":count("SELECT COUNT(*) FROM penalties WHERE substr(simulator_time,1,10)=?",(date,)),
        "co_incidents":count("SELECT COUNT(*) FROM alerts WHERE alert_type='HIGH CO LEVEL' AND substr(simulator_time,1,10)=?",(date,)),
        "broken":count("SELECT COUNT(*) FROM audit_log WHERE action='COMPONENT_BROKEN' AND substr(simulator_time,1,10)=?",(date,)),
        "repairs":count("SELECT COUNT(*) FROM maintenance_actions WHERE substr(simulator_time,1,10)=?",(date,)),
        "failed_logins":count("SELECT COUNT(*) FROM login_attempts WHERE success=0 AND substr(attempted_at,1,10)=?",(date,))
    }
    alerts=[dict(r) for r in conn.execute("SELECT * FROM alerts WHERE substr(simulator_time,1,10)=? ORDER BY id DESC LIMIT 100",(date,)).fetchall()]
    conn.close()
    return render_template_string(REPORT,date=date,summary=summary,alerts=alerts)

@admin_bp.route("/audit")
def audit_page():
    if session.get("role")!="Admin":return redirect("/")
    conn=db();rows=[dict(r) for r in conn.execute("SELECT * FROM audit_log ORDER BY id DESC LIMIT 500").fetchall()];conn.close()
    return render_template_string(AUDIT,rows=rows)
